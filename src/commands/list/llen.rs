use crate::commands::Context;
use crate::rdb::Value;
use crate::resp::encode_resp_error;
use std::io;

pub fn cmd_llen(args: &[String], ctx: &mut Context) -> io::Result<Vec<u8>> {
    println!("[cmd_llen] Received LLEN command with args: {:?}", args);

    if args.len() != 2 {
        println!("[cmd_llen] Invalid number of arguments.");
        return Ok(encode_resp_error("usage: LLEN <key>"));
    }

    let key = &args[1];
    let map = ctx.store.lock().unwrap();
    println!("[cmd_llen] Checking length of key '{}'", key);

    let response = match map.get(key) {
        Some((Value::List(list), _)) => {
            println!("[cmd_llen] List found with {} element(s)", list.len());
            format!(":{}\r\n", list.len()).into_bytes()
        }
        Some((Value::String(_), _)) | Some((Value::Stream(_), _)) => {
            eprintln!("[cmd_llen] WRONGTYPE: Key '{}' is not a list", key);
            encode_resp_error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => {
            println!("[cmd_llen] Key '{}' not found. Returning 0.", key);
            b":0\r\n".to_vec()
        }
    };

    Ok(response)
}
