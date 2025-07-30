use crate::commands::Context;
use crate::rdb::Value;
use crate::resp::{write_error, write_simple_string};
use std::io;
use std::net::TcpStream;
use std::time::{Duration, SystemTime};

/// SET <key> <value> [PX ms] → OK or error
pub fn cmd_set(
    out: &mut TcpStream,
    args: &[String],
    ctx: &Context,
) -> io::Result<()> {
    // validate argument count
    if args.len() != 3 && args.len() != 5 {
        write_error(out, "usage: SET <key> <val> [PX ms]")?;
        return Ok(());
    }

    let key = &args[1];
    let val = &args[2];
    let mut map = ctx.store.lock().unwrap();

    if args.len() == 3 {
        // simple set without expiry
        map.insert(key.clone(), (Value::String(val.clone()), None));
    } else {
        // parse PX milliseconds
        let ms = match args[4].parse::<u64>() {
            Ok(n) => n,
            Err(_) => {
                write_error(out, "PX must be integer")?;
                return Ok(());
            }
        };
        // compute expiry time
        let expiry = SystemTime::now()
            .checked_add(Duration::from_millis(ms))
            .unwrap();
        map.insert(key.clone(), (Value::String(val.clone()), Some(expiry)));
    }

    // acknowledge success
    write_simple_string(out, "OK")
}
