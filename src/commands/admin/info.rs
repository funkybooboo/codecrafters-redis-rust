use crate::commands::Context;
use crate::resp::{check_len, write_bulk_string};
use std::io;
use std::net::TcpStream;

/// INFO replication
pub fn cmd_info(out: &mut TcpStream, args: &[String], ctx: &mut Context) -> io::Result<()> {
    if !check_len(out, args, 2, "usage: INFO replication") {
        return Ok(());
    }
    if args[1].eq_ignore_ascii_case("replication") {
        let info = format!(
            "role:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}",
            ctx.cfg.role, ctx.cfg.master_replid, ctx.cfg.master_repl_offset,
        );
        write_bulk_string(out, &info)
    } else {
        write_bulk_string(out, "")
    }
}
