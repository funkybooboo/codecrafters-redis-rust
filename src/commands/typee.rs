use crate::commands::Context;
use crate::rdb::Value;
use crate::resp::{check_len, write_simple_string};
use std::io;
use std::net::TcpStream;
use std::time::SystemTime;

pub fn cmd_type(out: &mut TcpStream, args: &[String], ctx: &Context) -> io::Result<()> {
    if !check_len(out, args, 2, "usage: TYPE <key>") {
        return Ok(());
    }

    let key = &args[1];
    let map = ctx.store.lock().unwrap();

    let response = match map.get(key) {
        Some((val, opt_expiry)) => {
            if let Some(exp) = opt_expiry {
                if SystemTime::now() >= *exp {
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
        None => "none",
    };

    write_simple_string(out, response)
}
