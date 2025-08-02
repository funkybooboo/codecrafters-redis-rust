use crate::commands::Context;
use crate::resp::{encode_bulk_resp_string};
use std::io;

/// INFO replication
pub fn cmd_info(args: &[String], ctx: &mut Context) -> io::Result<Vec<u8>> {
    println!("[cmd_info] Received INFO command with args: {:?}", args);

    if args.len() != 2 || !args[1].eq_ignore_ascii_case("replication") {
        println!("[cmd_info] Invalid or unsupported INFO section");
        return Ok(encode_bulk_resp_string("")); // Empty response if unsupported
    }

    println!("[cmd_info] Generating replication info…");

    let info = format!(
        "role:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}",
        ctx.cfg.role,
        ctx.cfg.master_replid,
        ctx.master_repl_offset
    );

    println!("[cmd_info] INFO response:\n{}", info.replace("\r\n", "\\r\\n"));

    Ok(encode_bulk_resp_string(&info))
}
