use crate::commands::Context;
use crate::resp::{encode_simple_resp_string, encode_resp_error};
use std::io;

/// REPLCONF <option> <value>
pub fn cmd_replconf(args: &[String], _ctx: &mut Context) -> io::Result<Vec<u8>> {
    println!("[cmd_replconf] Received REPLCONF command with args: {:?}", args);

    if args.len() < 3 {
        println!("[cmd_replconf] Invalid number of arguments.");
        return Ok(encode_resp_error("usage: REPLCONF <option> <value>"));
    }

    let option = &args[1];
    let value = &args[2];
    println!("[cmd_replconf] Option: '{}', Value: '{}'", option, value);

    Ok(encode_simple_resp_string("OK"))
}
