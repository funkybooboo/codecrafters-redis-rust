use std::io;
use crate::resp::encode_simple_resp_string;
use crate::commands::Context;

pub fn cmd_multi(_args: &[String], ctx: &mut Context) -> io::Result<Vec<u8>> {
    println!("[cmd_multi] MULTI received, entering transaction mode");

    ctx.in_transaction = true;
    ctx.queued.clear();

    println!("[cmd_multi] transaction state initialized");

    Ok(encode_simple_resp_string("OK"))
}
