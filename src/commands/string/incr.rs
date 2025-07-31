use crate::commands::Context;
use crate::rdb::Value;
use crate::resp::write_error;
use std::io;
use std::io::Write;
use std::net::TcpStream;

/// INCR <key>
/// - Stage 1: key exists & numeric → increment  
/// - Stage 2: key missing → set to “1”  
/// - Stage 3: key exists but non-numeric → error
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

    // 2) Lock store
    let mut map = ctx.store.lock().unwrap();

    match map.get_mut(key) {
        Some((val, _)) => match val {
            Value::String(s) => {
                // Try parsing current value
                match s.parse::<i64>() {
                    // Stage 1: numeric → increment
                    Ok(n) => {
                        let new = n + 1;
                        *s = new.to_string();
                        write!(out, ":{new}\r\n")?;
                    }
                    // Stage 3: non-numeric → error
                    Err(_) => {
                        write_error(out, "value is not an integer or out of range")?;
                    }
                }
            }
            // Stage 3: wrong type → same error
            _ => {
                write_error(out, "value is not an integer or out of range")?;
            }
        },
        // Stage 2: key missing → set to 1
        None => {
            map.insert(key.clone(), (Value::String("1".into()), None));
            write!(out, ":1\r\n")?;
        }
    }

    Ok(())
}
