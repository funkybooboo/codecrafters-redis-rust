use crate::commands::Context;
use crate::resp::{check_len, write_bulk_string, write_error};
use std::io::{self, Write};
use std::net::TcpStream;

/// KEYS "*"
pub fn cmd_keys(out: &mut TcpStream, args: &[String], ctx: &mut Context) -> io::Result<()> {
    if !check_len(out, args, 2, "usage: KEYS *") {
        return Ok(());
    }
    if args[1] != "*" {
        write_error(out, "only '*' supported")?;
        return Ok(());
    }

    let map = ctx.store.lock().unwrap();
    let mut ks: Vec<&String> = map.keys().collect();
    ks.sort();

    write!(out, "*{}\r\n", ks.len())?;
    for &k in &ks {
        write_bulk_string(out, k)?;
    }
    Ok(())
}
