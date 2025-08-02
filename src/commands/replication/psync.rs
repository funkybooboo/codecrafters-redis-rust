use crate::commands::Context;
use crate::rdb::EMPTY_RDB;
use crate::resp::{encode_simple_resp_string, encode_resp_error};
use std::io;

pub fn cmd_psync(args: &[String], ctx: &mut Context) -> io::Result<Vec<u8>> {
    println!("[cmd_psync] Received PSYNC with args: {:?}", args);

    if args.len() != 3 {
        println!("[cmd_psync] Invalid number of arguments.");
        return Ok(encode_resp_error("usage: PSYNC <master_replid> <master_repl_offset>"));
    }

    if let Some(ref stream) = ctx.this_client {
        println!("[cmd_psync] Registering replica");
        ctx.replicas.lock().unwrap().push(stream.try_clone()?);
    } else {
        eprintln!("[cmd_psync] No this_client set — cannot register replica");
    }

    let full = format!(
        "FULLRESYNC {} {}",
        ctx.cfg.master_replid, ctx.master_repl_offset
    );
    println!("[cmd_psync] Responding with: {}", full);

    let mut out = encode_simple_resp_string(&full);
    out.extend_from_slice(format!("${}\r\n", EMPTY_RDB.len()).as_bytes());
    out.extend_from_slice(EMPTY_RDB);
    Ok(out)
}
