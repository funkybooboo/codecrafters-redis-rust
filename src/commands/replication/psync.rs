use crate::commands::Context;
use crate::rdb::EMPTY_RDB;
use crate::resp::{encode_simple_resp_string, encode_resp_error};
use std::io;

pub fn cmd_psync(args: &[String], ctx: &mut Context) -> io::Result<Vec<u8>> {
    println!("[cmd_psync] Received PSYNC command with args: {:?}", args);

    // Validate argument count
    if args.len() != 3 {
        eprintln!(
            "[cmd_psync] Invalid number of arguments: got {}, expected 3",
            args.len()
        );
        return Ok(encode_resp_error(
            "usage: PSYNC <master_replid> <master_repl_offset>",
        ));
    }

    // Extract and parse requested offset
    let requested_offset = args[2].parse::<usize>().unwrap_or(0);
    println!("[cmd_psync] Parsed requested offset: {}", requested_offset);

    // Attempt to register the current client as a replica
    if let Some(ref stream) = ctx.this_client {
        match stream.peer_addr() {
            Ok(peer) => {
                ctx.replicas
                    .lock()
                    .unwrap()
                    .insert(peer, (stream.try_clone()?, requested_offset));
                println!(
                    "[cmd_psync] Registered replica: {:?} with offset {}",
                    peer, requested_offset
                );
            }
            Err(e) => {
                eprintln!("[cmd_psync] Failed to get peer address: {}", e);
            }
        }
    } else {
        eprintln!("[cmd_psync] No active client stream found in Context.");
    }

    // Respond with FULLRESYNC header and initial RDB
    let full = format!(
        "FULLRESYNC {} {}",
        ctx.cfg.master_replid, ctx.master_repl_offset
    );
    println!("[cmd_psync] Responding to replica with: {}", full);

    let mut out = encode_simple_resp_string(&full);

    let rdb_len = EMPTY_RDB.len();
    out.extend_from_slice(format!("${}\r\n", rdb_len).as_bytes());
    out.extend_from_slice(EMPTY_RDB);

    println!(
        "[cmd_psync] Sent FULLRESYNC header and RDB payload ({} bytes)",
        rdb_len
    );

    Ok(out)
}
