use crate::commands::Context;
use crate::rdb::Value;
use crate::resp::write_error;
use std::io::{self, Write};
use std::net::TcpStream;

pub fn cmd_lpop(out: &mut TcpStream, args: &[String], ctx: &Context) -> io::Result<()> {
    if args.len() != 2 && args.len() != 3 {
        write_error(out, "usage: LPOP <key> [count]")?;
        return Ok(());
    }

    let key = &args[1];
    let count = if args.len() == 3 {
        match args[2].parse::<usize>() {
            Ok(n) if n > 0 => Some(n),
            _ => {
                write_error(out, "ERR count must be a positive integer")?;
                return Ok(());
            }
        }
    } else {
        None
    };

    let mut map = ctx.store.lock().unwrap();

    match map.get_mut(key) {
        Some((Value::List(ref mut list), _)) => {
            if list.is_empty() {
                match count {
                    Some(_) => write!(out, "*0\r\n")?, // empty array
                    None => write!(out, "$-1\r\n")?,   // null bulk string
                }
                return Ok(());
            }

            match count {
                Some(n) => {
                    let actual_n = n.min(list.len());
                    let mut removed = Vec::with_capacity(actual_n);
                    for _ in 0..actual_n {
                        removed.push(list.remove(0));
                    }

                    write!(out, "*{}\r\n", removed.len())?;
                    for item in removed {
                        write!(out, "${}\r\n{}\r\n", item.len(), item)?;
                    }
                }
                None => {
                    let popped = list.remove(0);
                    write!(out, "${}\r\n{}\r\n", popped.len(), popped)?;
                }
            }
        }
        Some((Value::String(_), _)) | Some((Value::Stream(_), _)) => {
            write_error(
                out,
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )?;
        }
        None => match count {
            Some(_) => write!(out, "*0\r\n")?,
            None => write!(out, "$-1\r\n")?,
        },
    }

    Ok(())
}
