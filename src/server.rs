use crate::commands::{dispatch_cmd, is_write_cmd};
use crate::resp::{read_resp_array, write_resp_array, write_simple_resp_string};
use crate::role::Role;
use crate::Context;

use std::{
    io::{self, BufReader, Write},
    net::TcpStream,
    thread,
};

pub fn serve_client_connection(stream: TcpStream, mut ctx: Context) -> io::Result<()> {
    let peer = stream
        .peer_addr()
        .unwrap_or_else(|_| "[unknown]".parse().unwrap());
    println!("[handle_client] New client connected: {:?}", peer);

    // So things like PUBLISH can reach back to this client
    ctx.this_client = Some(stream.try_clone()?);

    // We keep two clones of the socket here: one for reading, one for writing.
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = stream;

    while let Some(args) = read_resp_array(&mut reader)? {
        if args.is_empty() {
            continue;
        }
        let cmd = args[0].to_uppercase();

        // — Queuing inside MULTI/EXEC —
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

        // — Master: bump offset & propagate writes —
        if !ctx.in_transaction
            && ctx.cfg.role == Role::Master
            && is_write_cmd(&cmd)
        {
            // 1) bump
            ctx.master_repl_offset += 1;
            println!(
                "[handle_client] master_repl_offset now {} after '{}'",
                ctx.master_repl_offset, cmd
            );

            // 2) propagate the raw RESP array to each replica
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

        // — Execute the command locally & reply to client —
        println!("[handle_client] Dispatching '{}' for {:?}", cmd, peer);
        dispatch_cmd(&cmd, &mut writer, &args, &mut ctx)?;
        writer.flush()?;

        // ——— **NEW**: once PSYNC is done, spawn a reader thread for that link and return ———
        if ctx.cfg.role == Role::Master && cmd.eq_ignore_ascii_case("PSYNC") {
            println!("[handle_client] PSYNC complete, handing off replication link");

            // `reader` is a BufReader<TcpStream>; pull out the inner TcpStream
            let repl_stream = reader.into_inner();
            // clone the Context so our new thread can update the same `ctx.replicas`
            let ctx_for_reader = ctx.clone();

            thread::spawn(move || {
                let mut buf = BufReader::new(repl_stream);
                // keep reading the replica’s RESP frames
                while let Ok(Some(args)) = read_resp_array(&mut buf) {
                    // look for: REPLCONF ACK <offset>
                    if args.len() == 3
                        && args[0].eq_ignore_ascii_case("REPLCONF")
                        && args[1].eq_ignore_ascii_case("ACK")
                    {
                        if let Ok(peer_addr) = buf.get_ref().peer_addr() {
                            if let Ok(offset) = args[2].parse::<usize>() {
                                let mut reps = ctx_for_reader.replicas.lock().unwrap();
                                if let Some((_, last_ack)) = reps.get_mut(&peer_addr) {
                                    *last_ack = offset;
                                    println!(
                                        "[replication_reader] {} ACKed offset {}",
                                        peer_addr, offset
                                    );
                                }
                            }
                        }
                    }
                }
                println!("[replication_reader] replication link closed");
            });

            // return, dropping the `writer` clone here; the only live handles
            // to that socket are now the one in `ctx.replicas` (for writes)
            // and the one in our new reader thread.
            return Ok(());
        }
    }

    println!("[handle_client] Client {:?} disconnected.", peer);
    Ok(())
}
