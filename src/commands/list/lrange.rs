use crate::commands::Context;
use crate::rdb::Value;
use crate::resp::encode_resp_error;
use std::io;

pub fn cmd_lrange(args: &[String], ctx: &mut Context) -> io::Result<Vec<u8>> {
    println!("[cmd_lrange] Received LRANGE command with args: {:?}", args);

    if args.len() != 4 {
        println!("[cmd_lrange] Invalid argument count.");
        return Ok(encode_resp_error("usage: LRANGE <key> <start> <stop>"));
    }

    let key = &args[1];
    let start_raw = args[2].parse::<isize>().unwrap_or(isize::MAX);
    let stop_raw = args[3].parse::<isize>().unwrap_or(isize::MAX);

    if start_raw == isize::MAX || stop_raw == isize::MAX {
        eprintln!("[cmd_lrange] Invalid index: start='{}', stop='{}'", args[2], args[3]);
        return Ok(encode_resp_error("ERR start/stop must be integers"));
    }

    println!("[cmd_lrange] Parsed indices: start={}, stop={}", start_raw, stop_raw);
    let map = ctx.store.lock().unwrap();

    match map.get(key) {
        Some((Value::List(list), _)) => {
            let len = list.len() as isize;
            println!("[cmd_lrange] List '{}' found with length {}", key, len);

            let start = if start_raw < 0 {
                (len + start_raw).max(0)
            } else {
                start_raw
            } as usize;

            let stop = if stop_raw < 0 {
                (len + stop_raw).max(0)
            } else {
                stop_raw
            } as usize;

            println!("[cmd_lrange] Normalized range: start={}, stop={}", start, stop);

            if start > stop || start >= list.len() {
                println!("[cmd_lrange] Empty result: start > stop or out of bounds");
                return Ok(b"*0\r\n".to_vec());
            }

            let stop = stop.min(list.len() - 1);
            let slice = &list[start..=stop];
            println!("[cmd_lrange] Returning {} item(s) from index {} to {}", slice.len(), start, stop);

            let mut resp = format!("*{}\r\n", slice.len()).into_bytes();
            for item in slice {
                println!("[cmd_lrange] -> '{}'", item);
                resp.extend_from_slice(format!("${}\r\n{}\r\n", item.len(), item).as_bytes());
            }
            Ok(resp)
        }
        Some((Value::String(_), _)) | Some((Value::Stream(_), _)) => {
            eprintln!("[cmd_lrange] WRONGTYPE: key '{}' is not a list", key);
            Ok(encode_resp_error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            ))
        }
        None => {
            println!("[cmd_lrange] Key '{}' not found. Returning empty array.", key);
            Ok(b"*0\r\n".to_vec())
        }
    }
}
