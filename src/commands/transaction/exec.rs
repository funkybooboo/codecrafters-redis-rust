use std::io;
use std::io::Write;
use std::net::TcpStream;
use crate::resp::write_error;
use crate::commands::Context;

pub fn cmd_exec(
    out: &mut TcpStream,
    _args: &[String],
    ctx: &mut Context,
) -> io::Result<()> {
    if !ctx.in_transaction {
        write_error(out, "EXEC without MULTI")?;
    } else if ctx.queued.is_empty() {
        out.write_all(b"*0\r\n")?;
        ctx.in_transaction = false;
    } else {
        // future: replay queued
        ctx.in_transaction = false;
        ctx.queued.clear();
    }
    Ok(())
}
