use crate::commands::Context;
use crate::resp::write_simple_string;
use std::io;
use std::net::TcpStream;

/// PING -> +PONG
pub fn cmd_ping(out: &mut TcpStream, _args: &[String], _ctx: &Context) -> io::Result<()> {
    write_simple_string(out, "PONG")
}
