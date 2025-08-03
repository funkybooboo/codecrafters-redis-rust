use crate::commands::{dispatch_cmd, is_write_cmd};
use crate::resp::{read_resp_array, write_resp_array, write_simple_resp_string};
use crate::role::Role;
use crate::Context;
use std::{
    io::{self, BufReader, Write},
    net::TcpStream,
};

pub fn handle_client(stream: TcpStream, mut ctx: Context) -> io::Result<()> {
    let peer = stream.peer_addr().unwrap_or_else(|_| "[unknown address]".parse().unwrap());
    println!("[handle_client] New client connected: {:?}", peer);

    ctx.this_client = Some(stream.try_clone()?);

    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = stream;

    while let Some(args) = read_resp_array(&mut reader)? {
        if args.is_empty() {
            println!("[handle_client] Empty command received from {:?}, skipping.", peer);
            continue;
        }

        let cmd = args[0].to_uppercase();
        let cmd_args = &args[1..];
        println!(
            "[handle_client] Received command '{}' from {:?} with args: {:?}",
            cmd, peer, cmd_args
        );

        // Handle transaction queuing
        if ctx.in_transaction && cmd != "MULTI" && cmd != "EXEC" && cmd != "DISCARD" {
            println!(
                "[handle_client] In transaction mode — queuing command '{}' from {:?}",
                cmd, peer
            );
            ctx.queued.push((cmd.clone(), args.to_vec()));
            write_simple_resp_string(&mut writer, "QUEUED")?;
            writer.flush()?;
            continue;
        }

        println!("[handle_client] Dispatching command '{}' for {:?}", cmd, peer);
        if let Err(e) = dispatch_cmd(&cmd, &mut writer, &args, &mut ctx) {
            eprintln!("[handle_client] Error dispatching command '{}': {} (from {:?})", cmd, e, peer);
            break;
        }
        writer.flush()?;
        println!("[handle_client] Command '{}' executed successfully for {:?}", cmd, peer);

        // Write replication
        if !ctx.in_transaction && ctx.cfg.role == Role::Master && is_write_cmd(&cmd) {
            println!("[handle_client] Propagating write command '{}' to replicas", cmd);

            let items: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
            let mut replicas = ctx.replicas.lock().unwrap();

            let before = replicas.len();
            replicas.retain_mut(|rep| {
                match write_resp_array(rep, &items) {
                    Ok(_) => {
                        println!("[handle_client] Write propagated to replica {:?}", rep.peer_addr().unwrap_or_else(|_| "[unknown]".parse().unwrap()));
                        true
                    }
                    Err(e) => {
                        eprintln!("[handle_client] Failed to write to replica: {} — removing it", e);
                        false
                    }
                }
            });
            let after = replicas.len();
            if before != after {
                println!("[handle_client] Replica list updated. Active replicas: {}", after);
            }
        }
    }

    println!("[handle_client] Client {:?} disconnected.", peer);
    Ok(())
}
