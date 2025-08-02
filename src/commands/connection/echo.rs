use crate::commands::Context;
use crate::resp::{encode_bulk_resp_string, encode_resp_error};
use std::io;

/// ECHO <msg> -> BulkString(msg)
pub fn cmd_echo(args: &[String], _ctx: &mut Context) -> io::Result<Vec<u8>> {
    println!("[cmd_echo] Received ECHO command with args: {:?}", args);

    if args.len() != 2 {
        println!("[cmd_echo] Invalid number of arguments.");
        return Ok(encode_resp_error("usage: ECHO <msg>"));
    }

    println!("[cmd_echo] Echoing message: '{}'", args[1]);
    Ok(encode_bulk_resp_string(&args[1]))
}
