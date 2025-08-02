use crate::commands::Context;
use crate::rdb::Value;
use crate::resp::encode_resp_error;
use std::io;

pub fn cmd_lpush(args: &[String], ctx: &mut Context) -> io::Result<Vec<u8>> {
    println!("[cmd_lpush] Received LPUSH command with args: {:?}", args);

    if args.len() < 3 {
        println!("[cmd_lpush] Invalid number of arguments.");
        return Ok(encode_resp_error("usage: LPUSH <key> <value> [value ...]"));
    }

    let key = &args[1];
    let values = &args[2..];
    println!("[cmd_lpush] Target key: '{}', values to push: {:?}", key, values);

    let mut store = ctx.store.lock().unwrap();

    let new_len = match store.get_mut(key) {
        Some((Value::List(ref mut list), _)) => {
            println!("[cmd_lpush] Key exists and is a list. Prepending {} item(s).", values.len());
            for v in values {
                println!("[cmd_lpush] -> Inserting at front: '{}'", v);
                list.insert(0, v.clone());
            }
            list.len()
        }
        Some((Value::String(_), _)) | Some((Value::Stream(_), _)) => {
            eprintln!("[cmd_lpush] WRONGTYPE: Key '{}' is not a list", key);
            return Ok(encode_resp_error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            ));
        }
        None => {
            println!("[cmd_lpush] Key does not exist. Creating new list.");
            let mut new_list = Vec::with_capacity(values.len());
            for v in values.iter().rev() {
                println!("[cmd_lpush] -> Adding to new list (reversed): '{}'", v);
                new_list.push(v.clone());
            }
            let len = new_list.len();
            store.insert(key.clone(), (Value::List(new_list), None));
            len
        }
    };

    Ok(format!(":{}\r\n", new_len).into_bytes())
}
