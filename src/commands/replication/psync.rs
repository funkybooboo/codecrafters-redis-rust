use crate::commands::Context;
use crate::rdb::EMPTY_RDB;
use crate::resp::{check_len, write_simple_string};
use crate::role::Role;
use std::io;
use std::io::Write;
use std::net::TcpStream;

/// PSYNC <master_replid> <master_repl_offset>
///   → +FULLRESYNC <replid> 0\r\n
///   → $<len>\r\n<empty RDB bytes>
pub fn cmd_psync(stream: &mut TcpStream, args: &[String], ctx: &mut Context) -> io::Result<()> {
    // 1) Validate args
    if !check_len(
        stream,
        args,
        3,
        "usage: PSYNC <master_replid> <master_repl_offset>",
    ) {
        return Ok(());
    }

    // 2) Send "+FULLRESYNC <id> <offset>\r\n"
    let full = format!(
        "FULLRESYNC {} {}",
        ctx.cfg.master_replid, ctx.cfg.master_repl_offset
    );
    write_simple_string(stream, &full)?;

    // 3) Send empty RDB: length header + bytes
    write!(stream, "${}\r\n", EMPTY_RDB.len())?;
    stream.write_all(EMPTY_RDB)?;

    // 4) If we are master, register this replica for later propagation
    if ctx.cfg.role == Role::Master {
        let mut reps = ctx.replicas.lock().unwrap();
        // clone the connection so future SETs can fan out here
        reps.push(stream.try_clone()?);
    }

    Ok(())
}
