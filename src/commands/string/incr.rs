use crate::commands::Context;
use crate::rdb::Value;
use crate::resp::write_error;
use std::io;
use std::io::Write;
use std::net::TcpStream;

/// INCR <key>
/// Stage 2: if the key doesn’t exist, set it to 1
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
        // 3a) Key exists: only handle existing String values
        if let Value::String(s) = val {
            match s.parse::<i64>() {
                Ok(n) => {
                    let new = n + 1;
                    *s = new.to_string();
                    // integer reply
                    write!(out, ":{}\r\n", new)?;
                }
                Err(_) => {
                    // non-numeric string → error (stage 3 will refine this)
                    write_error(out, "ERR value is not an integer or out of range")?;
                }
            }
        } else {
            // wrong data type
            write_error(out, "ERR value is not an integer or out of range")?;
        }
    } else {
        // 3b) Key does not exist → set to 1
        map.insert(key.clone(), (Value::String("1".into()), None));
        // integer reply for newly-created key
        write!(out, ":1\r\n")?;
    }

    Ok(())
}
