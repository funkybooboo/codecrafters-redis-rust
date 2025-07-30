use crate::commands::Context;
use crate::rdb::Value;
use crate::resp::{check_len, write_error};
use std::io::{self, Write};
use std::net::TcpStream;

pub fn cmd_lrange(out: &mut TcpStream, args: &[String], ctx: &Context) -> io::Result<()> {
    if !check_len(out, args, 4, "usage: LRANGE <key> <start> <stop>") {
        return Ok(());
    }

    let key = &args[1];
    let start_raw = args[2].parse::<isize>().unwrap_or(isize::MAX);
    let stop_raw = args[3].parse::<isize>().unwrap_or(isize::MAX);

    if start_raw == isize::MAX || stop_raw == isize::MAX {
        write_error(out, "ERR start/stop must be integers")?;
        return Ok(());
    }

    let map = ctx.store.lock().unwrap();

    match map.get(key) {
        Some((Value::List(list), _)) => {
            let len = list.len() as isize;

            // Convert negative indexes
            let start = if start_raw < 0 {
                (len + start_raw).max(0)
            } else {
                start_raw
            } as usize;

            let stop = if stop_raw < 0 {
                (len + stop_raw).max(0)
            } else {
                stop_raw
            } as usize;

            // Edge cases
            if start > stop || start >= list.len() {
                write!(out, "*0\r\n")?;
                return Ok(());
            }

            let stop = stop.min(list.len() - 1);
            let slice = &list[start..=stop];

            write!(out, "*{}\r\n", slice.len())?;
            for item in slice {
                write!(out, "${}\r\n{}\r\n", item.len(), item)?;
            }
        }
        Some((Value::String(_), _)) | Some((Value::Stream(_), _)) => {
            write_error(
                out,
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )?;
        }
        None => {
            write!(out, "*0\r\n")?;
        }
    }

    Ok(())
}
