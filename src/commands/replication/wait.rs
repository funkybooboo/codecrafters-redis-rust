use std::io;
use crate::Context;

pub fn cmd_wait(_args: &[String], _ctx: &mut Context) -> io::Result<Vec<u8>> {
    // For now, always respond with 0
    Ok(b":0\r\n".to_vec())
}
