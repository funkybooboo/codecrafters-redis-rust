use crate::commands::Context;
use crate::rdb::EMPTY_RDB;
use crate::resp::{encode_simple_resp_string, encode_resp_error};
use std::io;

pub fn cmd_psync(args: &[String], ctx: &mut Context) -> io::Result<Vec<u8>> {
    println!("[cmd_psync] Received PSYNC command with args: {:?}", args);

    // Validate argument count
    if args.len() != 3 {
        eprintln!("[cmd_psync] Invalid number of arguments: got {}, expected 3", args.len());
        return Ok(encode_resp_error("usage: PSYNC <master_replid> <master_repl_offset>"));
    }

    // Attempt to register the current client as a replica
    if let Some(ref stream) = ctx.this_client {
        match stream.try_clone() {
            Ok(clone) => {
                ctx.replicas.lock().unwrap().push(clone);
                println!(
                    "[cmd_psync] Registered new replica from {:?}. Total replicas: {}",
                    stream.peer_addr().unwrap_or_else(|_| "unknown".parse().unwrap()),
                    ctx.replicas.lock().unwrap().len()
                );
            }
            Err(e) => {
                eprintln!("[cmd_psync] Failed to clone stream for replica: {}", e);
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
    out.extend_from_slice(format!("${}\r\n", EMPTY_RDB.len()).as_bytes());
    out.extend_from_slice(EMPTY_RDB);

    println!(
        "[cmd_psync] Sent FULLRESYNC header and RDB payload ({} bytes)",
        EMPTY_RDB.len()
    );

    Ok(out)
}
