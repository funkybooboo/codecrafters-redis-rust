use crate::commands::Context;
use crate::rdb::Value;
use crate::resp::write_error;
use std::io::{self, Write};
use std::net::TcpStream;

pub fn cmd_lpush(out: &mut TcpStream, args: &[String], ctx: &Context) -> io::Result<()> {
    if args.len() < 3 {
        write_error(out, "usage: LPUSH <key> <value> [value ...]")?;
        return Ok(());
    }

    let key = &args[1];
    let values = &args[2..];
    let mut store = ctx.store.lock().unwrap();

    match store.get_mut(key) {
        Some((Value::List(ref mut list), _)) => {
            for v in values {
                list.insert(0, v.clone());
            }
            write!(out, ":{}\r\n", list.len())?;
        }
        Some((Value::String(_), _)) | Some((Value::Stream(_), _)) => {
            write_error(
                out,
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )?;
        }
        None => {
            let mut new_list = Vec::with_capacity(values.len());
            for v in values.iter().rev() {
                new_list.push(v.clone());
            }
            store.insert(key.clone(), (Value::List(new_list), None));
            write!(out, ":{}\r\n", values.len())?;
        }
    }

    Ok(())
}
