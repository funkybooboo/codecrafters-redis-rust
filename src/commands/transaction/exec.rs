use std::io;
use std::io::Write;
use std::net::TcpStream;
use crate::resp::write_error;
use crate::commands::Context;

/// EXEC → if no MULTI, error; if MULTI but empty queue, return *0; otherwise
/// replay the queued commands (next stage).
pub fn cmd_exec(
    out: &mut TcpStream,
    _args: &[String],
    ctx: &mut Context,
) -> io::Result<()> {
    if !ctx.in_transaction {
        // never called MULTI
        write_error(out, "EXEC without MULTI")?;
    } else {
        if ctx.queued.is_empty() {
            // empty transaction
            out.write_all(b"*0\r\n")?;
        } else {
            // future stage: replay queued commands
            // let count = ctx.queued.len();
            // write_simple_string(out, &format!("*{}", count))?;
            // for (name, args) in ctx.queued.drain(..) {
            //     dispatch_cmd(&name, out, &args, ctx)?;
            // }
        }
        ctx.in_transaction = false;
        ctx.queued.clear();
    }
    Ok(())
}
