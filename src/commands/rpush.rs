use std::io;
use std::io::Write;
use std::net::TcpStream;
use crate::commands::Context;
use crate::rdb::Value;
use crate::resp::write_error;

pub fn cmd_rpush(
    out: &mut TcpStream,
    args: &[String],
    ctx: &Context,
) -> io::Result<()> {
    if args.len() < 3 {
        write_error(out, "usage: RPUSH <key> <value> [value ...]")?;
        return Ok(());
    }

    let key = &args[1];
    let values = &args[2..];
    let mut store = ctx.store.lock().unwrap();

    match store.get_mut(key) {
        Some((Value::List(ref mut list), _)) => {
            list.extend_from_slice(values);
            write!(out, ":{}\r\n", list.len())?;
        }
        Some((Value::String(_), _)) | Some((Value::Stream(_), _)) => {
            write_error(out, "WRONGTYPE Operation against a key holding the wrong kind of value")?;
            return Ok(());
        }
        None => {
            store.insert(key.clone(), (Value::List(values.to_vec()), None));
            write!(out, ":{}\r\n", values.len())?;
        }
    }

    let mut blockers = ctx.blocking.lock().unwrap();
    if let Some(waiters) = blockers.get_mut(key) {
        if !waiters.is_empty() {
            let mut client = waiters.remove(0); // FIFO: remove the first client
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
                    let _ = client.write_all(response.as_bytes());
                }
            }
        }

        // Cleanup if no more waiters for this key
        if waiters.is_empty() {
            blockers.remove(key);
        }
    }

    Ok(())
}
