use std::{
    io::{self, Write},
    net::TcpStream,
};
use crate::{
    commands::{Context, dispatch_cmd},
    resp::write_error,
};

/// EXEC → if no MULTI, error; otherwise execute every queued command
/// and return an array of their replies.
pub fn cmd_exec(
    out: &mut TcpStream,
    _args: &[String],
    ctx: &mut Context,
) -> io::Result<()> {
    if !ctx.in_transaction {
        // EXEC without a prior MULTI
        write_error(out, "EXEC without MULTI")?;
        return Ok(());
    }

    // 1) Swap out the queued commands, leaving ctx.queued empty
    let queued = std::mem::take(&mut ctx.queued);

    // 2) Write the array header
    write!(out, "*{}\r\n", queued.len())?;

    // 3) Replay each queued command
    //    (now `queued` is owned by us, no borrow of ctx remains)
    for (cmd_name, cmd_args) in queued {
        dispatch_cmd(&cmd_name, out, &cmd_args, ctx)?;
    }

    // 4) Tear down the transaction
    ctx.in_transaction = false;
    // ctx.queued is already empty

    Ok(())
}
