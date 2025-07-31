use std::io;
use std::net::TcpStream;
use crate::resp::write_error;
use crate::commands::Context;

/// EXEC
/// If MULTI hasn’t been called yet, return `-ERR EXEC without MULTI`
pub fn cmd_exec(
    out: &mut TcpStream,
    _args: &[String],
    _ctx: &Context,
) -> io::Result<()> {
    write_error(out, "EXEC without MULTI")?;
    Ok(())
}
