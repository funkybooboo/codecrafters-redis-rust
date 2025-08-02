use crate::commands::Context;
use crate::rdb::Value;
use crate::resp::{encode_bulk_resp_string, encode_resp_error};
use std::io;
use std::time::SystemTime;

/// GET key -> BulkString or NullBulk
pub fn cmd_get(args: &[String], ctx: &mut Context) -> io::Result<Vec<u8>> {
    println!("[cmd_get] called with args: {:?}", args);

    if args.len() != 2 {
        println!("[cmd_get] invalid argument count");
        return Ok(encode_resp_error("usage: GET <key>"));
    }

    let key = &args[1];
    println!("[cmd_get] looking up key: {}", key);

    let mut map = ctx.store.lock().unwrap();
    if let Some((val, opt_expiry)) = map.get(key).cloned() {
        if let Some(exp) = opt_expiry {
            if SystemTime::now() >= exp {
                println!("[cmd_get] key expired: {}", key);
                map.remove(key);
                return Ok(b"$-1\r\n".to_vec()); // Null bulk
            }
        }

        match val {
            Value::String(s) => {
                println!("[cmd_get] found string value for key: {}", key);
                Ok(encode_bulk_resp_string(&s))
            }
            _ => {
                println!("[cmd_get] wrong type for key: {}", key);
                Ok(encode_resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"))
            }
        }
    } else {
        println!("[cmd_get] key not found: {}", key);
        Ok(b"$-1\r\n".to_vec()) // Null bulk
    }
}
