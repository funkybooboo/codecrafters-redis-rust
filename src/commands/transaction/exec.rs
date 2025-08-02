use std::io;
use crate::commands::{Context, ALL_CMDS};
use crate::resp::{encode_resp_array, encode_resp_error};

/// EXEC → if no MULTI, error; otherwise execute every queued command
/// and emit them as a RESP array, then clear the transaction.
pub fn cmd_exec(_args: &[String], ctx: &mut Context) -> io::Result<Vec<u8>> {
    println!("[cmd_exec] called");

    if !ctx.in_transaction {
        println!("[cmd_exec] error: EXEC called without MULTI");
        return Ok(encode_resp_error("EXEC without MULTI"));
    }

    let queued = std::mem::take(&mut ctx.queued);
    println!("[cmd_exec] executing {} queued command(s)", queued.len());

    let mut responses = Vec::with_capacity(queued.len());

    for (cmd_name, cmd_args) in queued {
        println!("[cmd_exec] dispatching command: {} {:?}", cmd_name, cmd_args);
        if let Some(cmd_fn) = ALL_CMDS.get(&cmd_name.to_uppercase()) {
            match cmd_fn(&cmd_args, ctx) {
                Ok(resp) => responses.push(resp),
                Err(_) => {
                    println!("[cmd_exec] command '{}' failed", cmd_name);
                    responses.push(b"-ERR command failed\r\n".to_vec());
                }
            }
        } else {
            println!("[cmd_exec] unknown command: {}", cmd_name);
            responses.push(b"-ERR unknown command\r\n".to_vec());
        }
    }

    ctx.in_transaction = false;
    println!("[cmd_exec] transaction complete, state cleared");

    Ok(encode_resp_array(&responses))
}
