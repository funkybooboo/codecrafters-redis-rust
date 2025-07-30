use std::{io, thread};
use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;
use crate::commands::Context;
use crate::rdb::Value;
use crate::resp::write_error;

pub fn cmd_blpop(
    out: &mut TcpStream,
    args: &[String],
    ctx: &Context,
) -> io::Result<()> {
    if args.len() != 3 {
        write_error(out, "usage: BLPOP <key> <timeout>")?;
        return Ok(());
    }

    let key = args[1].clone();
    let timeout_secs: f64 = match args[2].parse() {
        Ok(t) => t,
        Err(_) => {
            write_error(out, "ERR timeout must be a float")?;
            return Ok(());
        }
    };

    // Try immediate pop
    let mut store = ctx.store.lock().unwrap();
    if let Some((Value::List(ref mut list), _)) = store.get_mut(&key) {
        if !list.is_empty() {
            let val = list.remove(0);
            write!(
                out,
                "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                key.len(),
                key,
                val.len(),
                val
            )?;
            return Ok(());
        }
    }
    drop(store);

    // Prepare blocking
    let cloned_stream = out.try_clone()?;
    let client_addr = cloned_stream.peer_addr().ok(); // 🧠 get SocketAddr before move

    let mut blockers = ctx.blocking.lock().unwrap();
    blockers.entry(key.clone()).or_default().push(cloned_stream);
    drop(blockers);

    // Timeout handler
    if timeout_secs > 0.0 {
        let key = key.clone();
        let blocking = Arc::clone(&ctx.blocking);

        thread::spawn(move || {
            thread::sleep(Duration::from_secs_f64(timeout_secs));

            let mut blockers = blocking.lock().unwrap();
            if let Some(waiters) = blockers.get_mut(&key) {
                if let Some(index) = client_addr.and_then(|addr| {
                    waiters.iter().position(|s| s.peer_addr().ok() == Some(addr))
                }) {
                    if let Some(stream) = waiters.get_mut(index) {
                        let _ = stream.write_all(b"$-1\r\n");
                    }
                    waiters.remove(index);
                }

                if waiters.is_empty() {
                    blockers.remove(&key);
                }
            }
        });
    }

    Ok(())
}
