use crate::commands::Context;
use crate::rdb::Value;
use crate::resp::encode_resp_error;
use std::io;
use std::io::Write;

pub fn cmd_rpush(args: &[String], ctx: &mut Context) -> io::Result<Vec<u8>> {
    println!("[cmd_rpush] Received RPUSH command with args: {:?}", args);

    if args.len() < 3 {
        println!("[cmd_rpush] Invalid number of arguments.");
        return Ok(encode_resp_error("usage: RPUSH <key> <value> [value ...]"));
    }

    let key = &args[1];
    let values = &args[2..];
    println!("[cmd_rpush] Pushing to key '{}': {:?}", key, values);

    let mut store = ctx.store.lock().unwrap();
    let new_len;

    match store.get_mut(key) {
        Some((Value::List(ref mut list), _)) => {
            list.extend_from_slice(values);
            new_len = list.len();
            println!("[cmd_rpush] Appended {} item(s). New list length: {}", values.len(), new_len);
        }
        Some((Value::String(_), _)) | Some((Value::Stream(_), _)) => {
            eprintln!("[cmd_rpush] WRONGTYPE: Key '{}' holds incompatible value", key);
            return Ok(encode_resp_error("WRONGTYPE Operation against a key holding the wrong kind of value"));
        }
        None => {
            store.insert(key.clone(), (Value::List(values.to_vec()), None));
            new_len = values.len();
            println!("[cmd_rpush] Created new list with {} item(s).", new_len);
        }
    }

    // Handle blocking clients (BLPOP) waiting on this key
    let mut blockers = ctx.blocking.lock().unwrap();
    if let Some(waiters) = blockers.get_mut(key) {
        if !waiters.is_empty() {
            println!("[cmd_rpush] Unblocking 1 waiting client for key '{}'", key);
            let mut client = waiters.remove(0);

            if let Some((Value::List(ref mut list), _)) = store.get_mut(key) {
                if !list.is_empty() {
                    let val = list.remove(0);
                    let response = format!(
                        "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                        key.len(),
                        key,
                        val.len(),
                        val
                    );
                    println!("[cmd_rpush] Sending '{}' to unblocked client", val);
                    let _ = client.write_all(response.as_bytes());
                }
            }
        }

        if waiters.is_empty() {
            println!("[cmd_rpush] No more waiters for key '{}', cleaning up", key);
            blockers.remove(key);
        }
    }

    Ok(format!(":{}\r\n", new_len).into_bytes())
}
