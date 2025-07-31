use crate::commands::Context;
use crate::resp::write_simple_string;
use std::io;
use std::net::TcpStream;

/// MULTI
/// Starts a transaction: just reply +OK
pub fn cmd_multi(out: &mut TcpStream, _args: &[String], _ctx: &Context) -> io::Result<()> {
    write_simple_string(out, "OK")
}
