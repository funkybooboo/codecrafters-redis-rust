use crate::commands::Context;
use crate::resp::{encode_bulk_resp_string, encode_resp_array, encode_resp_error};
use std::io;

/// KEYS "*"
pub fn cmd_keys(args: &[String], ctx: &mut Context) -> io::Result<Vec<u8>> {
    println!("[cmd_keys] Received KEYS command with args: {:?}", args);

    if args.len() != 2 {
        println!("[cmd_keys] Invalid number of arguments.");
        return Ok(encode_resp_error("usage: KEYS *"));
    }

    if args[1] != "*" {
        eprintln!("[cmd_keys] Unsupported pattern '{}'. Only '*' is allowed.", args[1]);
        return Ok(encode_resp_error("only '*' supported"));
    }

    let map = ctx.store.lock().unwrap();
    let mut ks: Vec<&String> = map.keys().collect();
    ks.sort();

    println!("[cmd_keys] Found {} key(s)", ks.len());
    for k in &ks {
        println!("[cmd_keys] Key: '{}'", k);
    }

    let chunks: Vec<Vec<u8>> = ks.iter().map(|k| encode_bulk_resp_string(k)).collect();
    Ok(encode_resp_array(&chunks))
}
