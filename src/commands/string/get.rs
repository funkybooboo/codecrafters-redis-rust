use crate::commands::Context;
use crate::rdb::Value;
use crate::resp::{check_len, write_bulk_string, write_error};
use std::io;
use std::io::Write;
use std::net::TcpStream;
use std::time::SystemTime;

/// GET key -> BulkString or NullBulk
pub fn cmd_get(out: &mut TcpStream, args: &[String], ctx: &mut Context) -> io::Result<()> {
    if !check_len(out, args, 2, "usage: GET <key>") {
        return Ok(());
    }

    let key = &args[1];
    let mut map = ctx.store.lock().unwrap();
    if let Some((val, opt_expiry)) = map.get(key).cloned() {
        if let Some(exp) = opt_expiry {
            if SystemTime::now() >= exp {
                map.remove(key);
                return out.write_all(b"$-1\r\n");
            }
        }
        match val {
            Value::String(s) => write_bulk_string(out, &s),
            _ => write_error(
                out,
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            ),
        }
    } else {
        out.write_all(b"$-1\r\n")
    }
}
