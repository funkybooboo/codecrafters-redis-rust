use std::io;
use crate::commands::Context;
use crate::resp::{encode_simple_resp_string, encode_resp_error};

/// DISCARD
/// Abort a transaction: clear the queue and reply +OK,
/// or error if there is no open MULTI.
pub fn cmd_discard(_args: &[String], ctx: &mut Context) -> io::Result<Vec<u8>> {
    println!("[cmd_discard] called");

    if !ctx.in_transaction {
        println!("[cmd_discard] error: DISCARD called without MULTI");
        return Ok(encode_resp_error("DISCARD without MULTI"));
    }

    println!("[cmd_discard] clearing {} queued command(s)", ctx.queued.len());
    ctx.queued.clear();
    ctx.in_transaction = false;
    println!("[cmd_discard] transaction aborted");

    Ok(encode_simple_resp_string("OK"))
}
