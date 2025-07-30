use std::io::{self, Write};
use std::net::TcpStream;
use crate::commands::Context;
use crate::rdb::Value;
use crate::resp::write_error;

/// INCR <key>
/// (Stage 1: only supports keys that already exist with numeric String values)
pub fn cmd_incr(
    out: &mut TcpStream,
    args: &[String],
    ctx: &Context,
) -> io::Result<()> {
    // 1) Validate args
    if args.len() != 2 {
        write_error(out, "usage: INCR <key>")?;
        return Ok(());
    }
    let key = &args[1];

    // 2) Lock store and look up key
    let mut map = ctx.store.lock().unwrap();
    if let Some((val, _expiry)) = map.get_mut(key) {
        // 3) Only handle existing String values
        if let Value::String(s) = val {
            // parse as 64-bit integer
            match s.parse::<i64>() {
                Ok(n) => {
                    let new = n + 1;
                    // store back as a String
                    *s = new.to_string();
                    // integer reply
                    write!(out, ":{}\r\n", new)?;
                }
                Err(_) => {
                    // later stages will handle non-numeric
                    write_error(out, "ERR value is not an integer or out of range")?;
                }
            }
        } else {
            // later stages will handle WRONGTYPE
            write_error(out, "ERR value is not an integer or out of range")?;
        }
    } else {
        // later stages will handle non-existent keys
        write_error(out, "ERR no such key")?;
    }

    Ok(())
}
