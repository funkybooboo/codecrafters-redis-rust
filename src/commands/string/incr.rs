use crate::commands::Context;
use crate::rdb::Value;
use crate::resp::{encode_resp_error};
use std::io;

/// INCR <key>
pub fn cmd_incr(args: &[String], ctx: &mut Context) -> io::Result<Vec<u8>> {
    println!("[cmd_incr] called with args: {:?}", args);

    // 1) Validate args
    if args.len() != 2 {
        println!("[cmd_incr] invalid number of arguments");
        return Ok(encode_resp_error("usage: INCR <key>"));
    }
    let key = &args[1];
    println!("[cmd_incr] operating on key: {}", key);

    // 2) Lock store
    let mut map = ctx.store.lock().unwrap();

    match map.get_mut(key) {
        Some((val, _)) => match val {
            Value::String(s) => {
                println!("[cmd_incr] found existing string value: {}", s);
                match s.parse::<i64>() {
                    Ok(n) => {
                        let new = n + 1;
                        *s = new.to_string();
                        println!("[cmd_incr] incremented value to: {}", new);
                        Ok(format!(":{}\r\n", new).into_bytes())
                    }
                    Err(_) => {
                        println!("[cmd_incr] value is not an integer");
                        Ok(encode_resp_error("value is not an integer or out of range"))
                    }
                }
            }
            _ => {
                println!("[cmd_incr] wrong type for key");
                Ok(encode_resp_error("value is not an integer or out of range"))
            }
        },
        None => {
            println!("[cmd_incr] key not found, setting to 1");
            map.insert(key.clone(), (Value::String("1".into()), None));
            Ok(b":1\r\n".to_vec())
        }
    }
}
