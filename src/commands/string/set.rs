use crate::commands::Context;
use crate::rdb::Value;
use crate::resp::{encode_resp_error, encode_simple_resp_string};
use std::io;
use std::time::{Duration, SystemTime};

/// SET <key> <value> [PX ms] → OK or error
pub fn cmd_set(args: &[String], ctx: &mut Context) -> io::Result<Vec<u8>> {
    println!("[cmd_set] called with args: {:?}", args);

    // validate argument count
    if args.len() != 3 && args.len() != 5 {
        println!("[cmd_set] invalid number of arguments");
        return Ok(encode_resp_error("usage: SET <key> <val> [PX ms]"));
    }

    let key = &args[1];
    let val = &args[2];
    println!("[cmd_set] setting key: '{}', value: '{}'", key, val);

    let mut map = ctx.store.lock().unwrap();

    if args.len() == 3 {
        // simple set without expiry
        println!("[cmd_set] no expiry provided");
        map.insert(key.clone(), (Value::String(val.clone()), None));
    } else {
        // validate optional args: must be "PX" and a number
        if args[3].to_uppercase() != "PX" {
            println!("[cmd_set] expected 'PX', found '{}'", args[3]);
            return Ok(encode_resp_error("expected PX for expiry"));
        }

        let ms = match args[4].parse::<u64>() {
            Ok(n) => n,
            Err(_) => {
                println!("[cmd_set] PX argument is not a valid integer: '{}'", args[4]);
                return Ok(encode_resp_error("PX must be integer"));
            }
        };

        let expiry = SystemTime::now()
            .checked_add(Duration::from_millis(ms))
            .unwrap();

        println!("[cmd_set] setting expiry in {}ms", ms);
        map.insert(key.clone(), (Value::String(val.clone()), Some(expiry)));
    }

    println!("[cmd_set] set successful");
    Ok(encode_simple_resp_string("OK"))
}
