use crate::commands::Context;
use crate::resp::encode_simple_resp_string;
use std::io;

/// PING -> +PONG
pub fn cmd_ping(_args: &[String], _ctx: &mut Context) -> io::Result<Vec<u8>> {
    println!("[cmd_ping] Received PING, responding with PONG");
    Ok(encode_simple_resp_string("PONG"))
}
