use crate::commands::Context;
use crate::rdb::Value;
use crate::resp::{encode_resp_error};
use std::io;

pub fn cmd_lpop(args: &[String], ctx: &mut Context) -> io::Result<Vec<u8>> {
    println!("[cmd_lpop] Received LPOP command with args: {:?}", args);

    if args.len() != 2 && args.len() != 3 {
        println!("[cmd_lpop] Invalid argument count.");
        return Ok(encode_resp_error("usage: LPOP <key> [count]"));
    }

    let key = &args[1];
    let count = if args.len() == 3 {
        match args[2].parse::<usize>() {
            Ok(n) if n > 0 => {
                println!("[cmd_lpop] Parsed count: {}", n);
                Some(n)
            }
            _ => {
                eprintln!("[cmd_lpop] Invalid count: '{}'", args[2]);
                return Ok(encode_resp_error("ERR count must be a positive integer"));
            }
        }
    } else {
        None
    };

    let mut map = ctx.store.lock().unwrap();
    println!("[cmd_lpop] Accessing key: '{}'", key);

    let response = match map.get_mut(key) {
        Some((Value::List(ref mut list), _)) => {
            if list.is_empty() {
                println!("[cmd_lpop] List is empty at key: '{}'", key);
                return Ok(match count {
                    Some(_) => b"*0\r\n".to_vec(),
                    None => b"$-1\r\n".to_vec(),
                });
            }

            match count {
                Some(n) => {
                    let actual_n = n.min(list.len());
                    println!("[cmd_lpop] Removing {} item(s) from list '{}'", actual_n, key);

                    let mut response = format!("*{}\r\n", actual_n).into_bytes();
                    for _ in 0..actual_n {
                        let item = list.remove(0);
                        println!("[cmd_lpop] -> '{}'", item);
                        response.extend_from_slice(format!("${}\r\n{}\r\n", item.len(), item).as_bytes());
                    }
                    response
                }
                None => {
                    let popped = list.remove(0);
                    println!("[cmd_lpop] Popped one item from '{}': '{}'", key, popped);
                    format!("${}\r\n{}\r\n", popped.len(), popped).into_bytes()
                }
            }
        }
        Some((Value::String(_), _)) | Some((Value::Stream(_), _)) => {
            eprintln!("[cmd_lpop] WRONGTYPE for key: '{}'", key);
            encode_resp_error("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        None => {
            println!("[cmd_lpop] Key '{}' not found. Returning empty/null response.", key);
            match count {
                Some(_) => b"*0\r\n".to_vec(),
                None => b"$-1\r\n".to_vec(),
            }
        }
    };

    Ok(response)
}
