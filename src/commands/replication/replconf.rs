use crate::commands::Context;
use crate::resp::{check_len, write_simple_string};
use std::io;
use std::net::TcpStream;

/// REPLCONF <option> <value>
pub fn cmd_replconf(out: &mut TcpStream, args: &[String], _ctx: &mut Context) -> io::Result<()> {
    if !check_len(out, args, 3, "usage: REPLCONF <option> <value>") {
        return Ok(());
    }
    write_simple_string(out, "OK")
}
