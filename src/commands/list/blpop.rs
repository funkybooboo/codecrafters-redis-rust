use crate::commands::Context;
use crate::rdb::Value;
use crate::resp::encode_resp_error;
use std::sync::Arc;
use std::time::Duration;
use std::{io, thread};
use std::io::Write;

pub fn cmd_blpop(args: &[String], ctx: &mut Context) -> io::Result<Vec<u8>> {
    println!("[cmd_blpop] Received BLPOP command with args: {:?}", args);

    if args.len() != 3 {
        println!("[cmd_blpop] Invalid argument count.");
        return Ok(encode_resp_error("usage: BLPOP <key> <timeout>"));
    }

    let key = args[1].clone();
    let timeout_secs: f64 = match args[2].parse() {
        Ok(t) => t,
        Err(_) => {
            eprintln!("[cmd_blpop] Invalid timeout '{}'", args[2]);
            return Ok(encode_resp_error("ERR timeout must be a float"));
        }
    };

    println!("[cmd_blpop] Attempting immediate pop from '{}'", key);
    let mut store = ctx.store.lock().unwrap();
    if let Some((Value::List(ref mut list), _)) = store.get_mut(&key) {
        if !list.is_empty() {
            let val = list.remove(0);
            println!("[cmd_blpop] Immediate pop successful. Returning value '{}'", val);

            let response = format!(
                "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                key.len(),
                key,
                val.len(),
                val
            );
            return Ok(response.into_bytes());
        } else {
            println!("[cmd_blpop] List at '{}' is empty. Blocking client.", key);
        }
    } else {
        println!("[cmd_blpop] Key '{}' does not exist or is not a list. Blocking client.", key);
    }
    drop(store);

    // Grab stream from context (you must have saved it per client previously)
    let stream = match &ctx.this_client {
        Some(s) => match s.try_clone() {
            Ok(clone) => clone,
            Err(e) => {
                eprintln!("[cmd_blpop] Failed to clone TcpStream: {}", e);
                return Ok(vec![]);
            }
        },
        None => {
            eprintln!("[cmd_blpop] No stream found in context");
            return Ok(vec![]);
        }
    };
    let client_addr = stream.peer_addr().ok();
    println!("[cmd_blpop] Blocking client from {:?}", client_addr);

    let mut blockers = ctx.blocking.lock().unwrap();
    blockers.entry(key.clone()).or_default().push(stream);
    println!("[cmd_blpop] Client added to blocking list for key '{}'", key);
    drop(blockers);

    if timeout_secs > 0.0 {
        println!("[cmd_blpop] Starting timeout thread for {:.3} seconds", timeout_secs);
        let key = key.clone();
        let blocking = Arc::clone(&ctx.blocking);

        thread::spawn(move || {
            thread::sleep(Duration::from_secs_f64(timeout_secs));

            println!("[cmd_blpop::timeout] Timeout triggered for key '{}'", key);
            let mut blockers = blocking.lock().unwrap();
            if let Some(waiters) = blockers.get_mut(&key) {
                if let Some(index) = client_addr.and_then(|addr| {
                    waiters.iter().position(|s| s.peer_addr().ok() == Some(addr))
                }) {
                    println!("[cmd_blpop::timeout] Timing out client {:?}", client_addr);
                    if let Some(stream) = waiters.get_mut(index) {
                        let _ = stream.write_all(b"$-1\r\n"); // Null bulk string
                    }
                    waiters.remove(index);
                }

                if waiters.is_empty() {
                    blockers.remove(&key);
                    println!("[cmd_blpop::timeout] No more clients blocking on '{}'", key);
                }
            }
        });
    }

    Ok(vec![]) // Don't send anything immediately
}
