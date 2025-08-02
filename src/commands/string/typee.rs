use crate::commands::Context;
use crate::rdb::Value;
use crate::resp::{encode_resp_error, encode_simple_resp_string};
use std::io;
use std::time::SystemTime;

pub fn cmd_type(args: &[String], ctx: &mut Context) -> io::Result<Vec<u8>> {
    println!("[cmd_type] called with args: {:?}", args);

    if args.len() != 2 {
        println!("[cmd_type] invalid argument count");
        return Ok(encode_resp_error("usage: TYPE <key>"));
    }

    let key = &args[1];
    let map = ctx.store.lock().unwrap();

    let response = match map.get(key) {
        Some((val, opt_expiry)) => {
            if let Some(exp) = opt_expiry {
                if SystemTime::now() >= *exp {
                    println!("[cmd_type] key '{}' is expired", key);
                    "none"
                } else {
                    match val {
                        Value::String(_) => "string",
                        Value::List(_) => "list",
                        Value::Stream(_) => "stream",
                    }
                }
            } else {
                match val {
                    Value::String(_) => "string",
                    Value::List(_) => "list",
                    Value::Stream(_) => "stream",
                }
            }
        }
        None => {
            println!("[cmd_type] key '{}' does not exist", key);
            "none"
        }
    };

    Ok(encode_simple_resp_string(response))
}
