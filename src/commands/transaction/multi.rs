use crate::commands::Context;
use crate::resp::write_simple_string;
use std::io;
use std::net::TcpStream;

/// MULTI
/// Starts a transaction: just reply +OK
pub fn cmd_multi(
    out: &mut TcpStream,
    _args: &[String],
    ctx: &mut Context,
) -> io::Result<()> {
    write_simple_string(out, "OK")?;
    ctx.in_transaction = true;
    ctx.queued.clear();
    Ok(())
}
