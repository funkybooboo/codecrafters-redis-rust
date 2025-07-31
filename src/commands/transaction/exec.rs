use std::{
    io::{self, Write},
    net::TcpStream,
};
use crate::{
    commands::{Context, dispatch_cmd},
    resp::write_error,
};

/// EXEC → if no MULTI, error; otherwise execute every queued command
/// (capturing each command’s own error or success reply) and emit them
/// as an array, then clear the transaction.
pub fn cmd_exec(
    out: &mut TcpStream,
    _args: &[String],
    ctx: &mut Context,
) -> io::Result<()> {
    // 1) If we never saw MULTI, error out
    if !ctx.in_transaction {
        write_error(out, "EXEC without MULTI")?;
        return Ok(());
    }

    // 2) Take ownership of the queued commands, leaving ctx.queued empty
    let queued = std::mem::take(&mut ctx.queued);

    // 3) Write the array header: one element per queued command
    write!(out, "*{}\r\n", queued.len())?;

    // 4) Replay each queued command in order.
    //    dispatch_cmd will write exactly one RESP reply (OK, error, integer, bulk, etc.)
    for (cmd_name, cmd_args) in queued {
        dispatch_cmd(&cmd_name, out, &cmd_args, ctx)?;
    }

    // 5) Tear down transaction state
    ctx.in_transaction = false;
    // ctx.queued is already empty

    Ok(())
}
