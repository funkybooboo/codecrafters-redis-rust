use crate::commands::Context;
use crate::rdb::Value;
use crate::resp::{check_len, write_error};
use std::io::{self, Write};
use std::net::TcpStream;

pub fn cmd_llen(out: &mut TcpStream, args: &[String], ctx: &Context) -> io::Result<()> {
    if !check_len(out, args, 2, "usage: LLEN <key>") {
        return Ok(());
    }

    let key = &args[1];
    let map = ctx.store.lock().unwrap();

    match map.get(key) {
        Some((Value::List(list), _)) => {
            write!(out, ":{}\r\n", list.len())?;
        }
        Some((Value::String(_), _)) | Some((Value::Stream(_), _)) => {
            write_error(
                out,
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )?;
        }
        None => {
            write!(out, ":0\r\n")?;
        }
    }

    Ok(())
}
