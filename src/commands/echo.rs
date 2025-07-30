use crate::commands::Context;
use crate::resp::{check_len, write_bulk_string};
use std::io;
use std::net::TcpStream;

/// ECHO <msg> -> BulkString(msg)
pub fn cmd_echo(out: &mut TcpStream, args: &[String], _ctx: &Context) -> io::Result<()> {
    if !check_len(out, args, 2, "usage: ECHO <msg>") {
        return Ok(());
    }
    write_bulk_string(out, &args[1])
}
