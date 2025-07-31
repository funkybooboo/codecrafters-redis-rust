use std::{io, net::TcpStream};
use crate::commands::Context;
use crate::resp::{write_simple_string, write_error};

/// DISCARD
/// Abort a transaction: clear the queue and reply +OK,
/// or error if there is no open MULTI.
pub fn cmd_discard(
    out: &mut TcpStream,
    _args: &[String],
    ctx: &mut Context,
) -> io::Result<()> {
    if !ctx.in_transaction {
        // no MULTI in progress → error
        write_error(out, "DISCARD without MULTI")?;
    } else {
        // drop all queued commands and leave transaction mode
        ctx.queued.clear();
        ctx.in_transaction = false;
        write_simple_string(out, "OK")?;
    }
    Ok(())
}
