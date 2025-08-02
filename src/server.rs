use crate::commands::{dispatch_cmd, is_write_cmd};
use crate::resp::{read_resp_array, write_resp_array, write_simple_resp_string};
use crate::role::Role;
use crate::Context;
use std::{
    io::{self, Write},
    net::TcpStream,
};

pub fn handle_client(stream: TcpStream, mut ctx: Context) -> io::Result<()> {
    println!("[handle_client] New client handler started: {:?}", stream.peer_addr());

    ctx.this_client = Some(stream.try_clone()?);

    let mut reader = io::BufReader::new(stream.try_clone()?);
    let mut writer = stream;

    while let Some(args) = read_resp_array(&mut reader)? {
        if args.is_empty() {
            println!("[handle_client] Empty command received, skipping.");
            continue;
        }

        let cmd = args[0].to_uppercase();
        println!("[handle_client] Received command: {} with args {:?}", cmd, &args[1..]);

        // Queue commands if in transaction (except MULTI/EXEC/DISCARD)
        if ctx.in_transaction && cmd != "MULTI" && cmd != "EXEC" && cmd != "DISCARD" {
            println!("[handle_client] In transaction mode, queuing command: {}", cmd);
            ctx.queued.push((cmd.clone(), args.to_vec()));
            write_simple_resp_string(&mut writer, "QUEUED")?;
            writer.flush()?;
            continue;
        }

        println!("[handle_client] Dispatching command: {}", cmd);
        dispatch_cmd(&cmd, &mut writer, &args, &mut ctx)?;
        writer.flush()?;
        println!("[handle_client] Command dispatched successfully: {}", cmd);

        // Propagate writes to replicas
        if !ctx.in_transaction && ctx.cfg.role == Role::Master && is_write_cmd(&cmd) {
            println!("[handle_client] Propagating write command to replicas: {}", cmd);
            let items: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
            let mut reps = ctx.replicas.lock().unwrap();
            let initial_count = reps.len();
            reps.retain_mut(|rep| write_resp_array(rep, &items).is_ok());
            let final_count = reps.len();
            if initial_count != final_count {
                println!("[handle_client] Replica removed due to error. Active replicas: {}", final_count);
            }
        }
    }

    println!("[handle_client] Client disconnected: {:?}", writer.peer_addr());
    Ok(())
}
