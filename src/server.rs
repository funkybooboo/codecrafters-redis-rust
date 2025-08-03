use crate::commands::{dispatch_cmd, is_write_cmd};
use crate::resp::{read_resp_array, write_resp_array, write_simple_resp_string};
use crate::role::Role;
use crate::Context;

use std::{
    io::{self, BufReader, Write},
    net::TcpStream,
};

pub fn serve_client_connection(stream: TcpStream, mut ctx: Context) -> io::Result<()> {
    let peer = stream
        .peer_addr()
        .unwrap_or_else(|_| "[unknown]".parse().unwrap());
    println!("[handle_client] New client connected: {:?}", peer);

    // so dispatch_cmd can reply directly if it needs to
    ctx.this_client = Some(stream.try_clone()?);

    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = stream;

    while let Some(args) = read_resp_array(&mut reader)? {
        if args.is_empty() {
            continue;
        }
        let cmd = args[0].to_uppercase();
        println!(
            "[handle_client] Received '{}' from {:?} args={:?}",
            cmd, peer, &args[1..]
        );

        // ——— inside MULTI/EXEC we just QUEUE ———
        if ctx.in_transaction
            && cmd != "MULTI"
            && cmd != "EXEC"
            && cmd != "DISCARD"
        {
            println!("[handle_client] Queued '{}' in transaction", cmd);
            ctx.queued.push((cmd.clone(), args.clone()));
            write_simple_resp_string(&mut writer, "QUEUED")?;
            writer.flush()?;
            continue;
        }

        // ——— MASTER, non‐transactional, write commands ———
        if !ctx.in_transaction
            && ctx.cfg.role == Role::Master
            && is_write_cmd(&cmd)
        {
            // 1) bump the replication offset
            ctx.master_repl_offset += 1;
            println!(
                "[handle_client] master_repl_offset now {} after '{}'",
                ctx.master_repl_offset, cmd
            );
            // 2) propagate the raw RESP array to all replicas
            let items: Vec<&str> = args.iter().map(String::as_str).collect();
            let mut reps = ctx.replicas.lock().unwrap();
            let mut to_remove = Vec::new();
            for (&addr, (rs, _)) in reps.iter_mut() {
                if let Err(e) = write_resp_array(rs, &items).and_then(|_| rs.flush()) {
                    eprintln!("[propagate] to {} failed: {}; removing", addr, e);
                    to_remove.push(addr);
                } else {
                    println!("[propagate] Write propagated to replica {}", addr);
                }
            }
            for addr in to_remove {
                reps.remove(&addr);
            }
        }

        // ——— execute locally & reply to the client ———
        println!(
            "[handle_client] Dispatching '{}' for {:?}",
            cmd, peer
        );
        dispatch_cmd(&cmd, &mut writer, &args, &mut ctx)?;
        writer.flush()?;
    }

    println!("[handle_client] Client {:?} disconnected.", peer);
    Ok(())
}
