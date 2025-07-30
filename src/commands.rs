use std::collections::HashMap;
use std::io::{self, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use std::thread;

use crate::config::ServerConfig;
use crate::resp::{
    write_simple_string, write_error, write_bulk_string, check_len,
};
use crate::rdb::{Store, Value, EMPTY_RDB};
use crate::role::Role;
use crate::server::BlockingList;

/// A little context bundling everything cmds might need
pub struct Context<'a> {
    pub cfg: &'a ServerConfig,
    pub store: &'a Store,
    pub replicas: Arc<Mutex<Vec<TcpStream>>>,
    pub blocking: BlockingList,
}

/// Every command has this signature
pub type CmdFn = fn(stream: &mut TcpStream, args: &[String], ctx: &Context) -> io::Result<()>;

// helper to know which commands should be fanned out
pub fn is_write_cmd(cmd: &str) -> bool {
    matches!(cmd, "SET" | "DEL" | "RPUSH")
}

/// Build the command registry once
pub fn make_registry() -> HashMap<String, CmdFn> {
    let mut m = HashMap::new();
    m.insert("PING".into(), cmd_ping as CmdFn);
    m.insert("ECHO".into(), cmd_echo as CmdFn);
    m.insert("SET".into(), cmd_set as CmdFn);
    m.insert("GET".into(), cmd_get as CmdFn);
    m.insert("CONFIG".into(), cmd_config as CmdFn);
    m.insert("KEYS".into(), cmd_keys as CmdFn);
    m.insert("INFO".into(), cmd_info as CmdFn);
    m.insert("REPLCONF".into(), cmd_replconf as CmdFn);
    m.insert("PSYNC".into(), cmd_psync as CmdFn);
    m.insert("RPUSH".into(), cmd_rpush as CmdFn);
    m.insert("LRANGE".into(), cmd_lrange as CmdFn);
    m.insert("LPUSH".into(), cmd_lpush as CmdFn);
    m.insert("LLEN".into(), cmd_llen as CmdFn);
    m.insert("LPOP".into(), cmd_lpop as CmdFn);
    m.insert("BLPOP".into(), cmd_blpop as CmdFn);
    m.insert("TYPE".into(), cmd_type as CmdFn);
    m
}

/// PING -> +PONG
pub fn cmd_ping(
    out: &mut TcpStream,
    _args: &[String],
    _ctx: &Context,
) -> io::Result<()> {
    write_simple_string(out, "PONG")
}

/// ECHO <msg> -> BulkString(msg)
pub fn cmd_echo(
    out: &mut TcpStream,
    args: &[String],
    _ctx: &Context,
) -> io::Result<()> {
    if !check_len(out, args, 2, "usage: ECHO <msg>") {
        return Ok(());
    }
    write_bulk_string(out, &args[1])
}

pub fn cmd_set(
    out: &mut TcpStream,
    args: &[String],
    ctx: &Context,
) -> io::Result<()> {
    match apply_set(args, &ctx.store) {
        Ok(()) => write_simple_string(out, "OK"),
        Err(e) => {
            // send back an error if args were malformed
            write_error(out, &e.to_string())?;
            Ok(())
        }
    }
}

/// Exactly the store-mutation logic from `cmd_set`, but no RESP output.
pub fn apply_set(
    args: &[String],
    store: &Store,
) -> io::Result<()> {
    // validate
    if args.len() != 3 && args.len() != 5 {
        // here we return Err so callers can decide what to do;
        // cmd_set will turn it into a write_error(out,…)
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: SET <key> <val> [PX ms]",
        ));
    }

    let key = &args[1];
    let val = &args[2];
    let mut map = store.lock().unwrap();

    if args.len() == 3 {
        map.insert(key.clone(), (Value::String(val.clone()), None));
    } else {
        // args == 5
        let ms = args[4].parse::<u64>().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "PX must be integer")
        })?;
        let expiry = SystemTime::now()
            .checked_add(Duration::from_millis(ms))
            .unwrap();
        map.insert(key.clone(), (Value::String(val.clone()), Some(expiry)));
    }

    Ok(())
}

/// GET key -> BulkString or NullBulk
pub fn cmd_get(
    out: &mut TcpStream,
    args: &[String],
    ctx: &Context,
) -> io::Result<()> {
    if !check_len(out, args, 2, "usage: GET <key>") {
        return Ok(());
    }

    let key = &args[1];
    let mut map = ctx.store.lock().unwrap();
    if let Some((val, opt_expiry)) = map.get(key).cloned() {
        if let Some(exp) = opt_expiry {
            if SystemTime::now() >= exp {
                map.remove(key);
                return out.write_all(b"$-1\r\n");
            }
        }
        match val {
            Value::String(s) => write_bulk_string(out, &s),
            _ => write_error(out, "WRONGTYPE Operation against a key holding the wrong kind of value"),
        }
    } else {
        out.write_all(b"$-1\r\n")
    }
}

/// CONFIG GET <dir|dbfilename>
pub fn cmd_config(
    out: &mut TcpStream,
    args: &[String],
    ctx: &Context,
) -> io::Result<()> {
    if !check_len(out, args, 3, "usage: CONFIG GET <dir|dbfilename>") {
        return Ok(());
    }

    let key = &args[2];
    let val = match key.as_str() {
        "dir"        => &ctx.cfg.dir,
        "dbfilename" => &ctx.cfg.dbfilename,
        _ => {
            write_error(out, "unknown config parameter")?;
            return Ok(());
        }
    };

    // array of two bulk-strings
    out.write_all("*2\r\n".to_string().as_bytes())?;
    write_bulk_string(out, key)?;
    write_bulk_string(out, val)
}

/// KEYS "*"
pub fn cmd_keys(
    out: &mut TcpStream,
    args: &[String],
    ctx: &Context,
) -> io::Result<()> {
    if !check_len(out, args, 2, "usage: KEYS *") {
        return Ok(());
    }
    if args[1] != "*" {
        write_error(out, "only '*' supported")?;
        return Ok(());
    }

    let map = ctx.store.lock().unwrap();
    let mut ks: Vec<&String> = map.keys().collect();
    ks.sort();

    write!(out, "*{}\r\n", ks.len())?;
    for &k in &ks {
        write_bulk_string(out, k)?;
    }
    Ok(())
}

/// INFO replication
pub fn cmd_info(
    out: &mut TcpStream,
    args: &[String],
    ctx: &Context,
) -> io::Result<()> {
    if !check_len(out, args, 2, "usage: INFO replication") {
        return Ok(());
    }
    if args[1].eq_ignore_ascii_case("replication") {
        let info = format!(
            "role:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}",
            ctx.cfg.role,
            ctx.cfg.master_replid,
            ctx.cfg.master_repl_offset,
        );
        write_bulk_string(out, &info)
    } else {
        write_bulk_string(out, "")
    }
}

/// REPLCONF <option> <value>
pub fn cmd_replconf(
    out: &mut TcpStream,
    args: &[String],
    _ctx: &Context,
) -> io::Result<()> {
    if !check_len(out, args, 3, "usage: REPLCONF <option> <value>") {
        return Ok(());
    }
    write_simple_string(out, "OK")
}

/// PSYNC <master_replid> <master_repl_offset>
///   → +FULLRESYNC <replid> 0\r\n
///   → $<len>\r\n<empty RDB bytes>
pub fn cmd_psync(
    stream: &mut TcpStream,
    args: &[String],
    ctx: &Context,
) -> io::Result<()> {
    // 1) Validate args
    if !check_len(stream, args, 3, "usage: PSYNC <master_replid> <master_repl_offset>") {
        return Ok(());
    }

    // 2) Send "+FULLRESYNC <id> <offset>\r\n"
    let full = format!("FULLRESYNC {} {}", ctx.cfg.master_replid, ctx.cfg.master_repl_offset);
    write_simple_string(stream, &full)?;

    // 3) Send empty RDB: length header + bytes
    write!(stream, "${}\r\n", EMPTY_RDB.len())?;
    stream.write_all(EMPTY_RDB)?;

    // 4) If we are master, register this replica for later propagation
    if ctx.cfg.role == Role::Master {
        let mut reps = ctx.replicas.lock().unwrap();
        // clone the connection so future SETs can fan out here
        reps.push(stream.try_clone()?);
    }

    Ok(())
}

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
        Some((Value::String(_), _)) => {
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

pub fn cmd_lrange(
    out: &mut TcpStream,
    args: &[String],
    ctx: &Context,
) -> io::Result<()> {
    if !check_len(out, args, 4, "usage: LRANGE <key> <start> <stop>") {
        return Ok(());
    }

    let key = &args[1];
    let start_raw = args[2].parse::<isize>().unwrap_or(isize::MAX);
    let stop_raw = args[3].parse::<isize>().unwrap_or(isize::MAX);

    if start_raw == isize::MAX || stop_raw == isize::MAX {
        write_error(out, "ERR start/stop must be integers")?;
        return Ok(());
    }

    let map = ctx.store.lock().unwrap();

    match map.get(key) {
        Some((Value::List(list), _)) => {
            let len = list.len() as isize;

            // Convert negative indexes
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

            // Edge cases
            if start > stop || start >= list.len() {
                write!(out, "*0\r\n")?;
                return Ok(());
            }

            let stop = stop.min(list.len() - 1);
            let slice = &list[start..=stop];

            write!(out, "*{}\r\n", slice.len())?;
            for item in slice {
                write!(out, "${}\r\n{}\r\n", item.len(), item)?;
            }
        }
        Some((Value::String(_), _)) => {
            write_error(out, "WRONGTYPE Operation against a key holding the wrong kind of value")?;
        }
        None => {
            write!(out, "*0\r\n")?;
        }
    }

    Ok(())
}

pub fn cmd_lpush(
    out: &mut TcpStream,
    args: &[String],
    ctx: &Context,
) -> io::Result<()> {
    if args.len() < 3 {
        write_error(out, "usage: LPUSH <key> <value> [value ...]")?;
        return Ok(());
    }

    let key = &args[1];
    let values = &args[2..];
    let mut store = ctx.store.lock().unwrap();

    match store.get_mut(key) {
        Some((Value::List(ref mut list), _)) => {
            for v in values {
                list.insert(0, v.clone());
            }
            write!(out, ":{}\r\n", list.len())?;
        }
        Some((Value::String(_), _)) => {
            write_error(out, "WRONGTYPE Operation against a key holding the wrong kind of value")?;
        }
        None => {
            let mut new_list = Vec::with_capacity(values.len());
            for v in values.iter().rev() {
                new_list.push(v.clone());
            }
            store.insert(key.clone(), (Value::List(new_list), None));
            write!(out, ":{}\r\n", values.len())?;
        }
    }

    Ok(())
}

pub fn cmd_llen(
    out: &mut TcpStream,
    args: &[String],
    ctx: &Context,
) -> io::Result<()> {
    if !check_len(out, args, 2, "usage: LLEN <key>") {
        return Ok(());
    }

    let key = &args[1];
    let map = ctx.store.lock().unwrap();

    match map.get(key) {
        Some((Value::List(list), _)) => {
            write!(out, ":{}\r\n", list.len())?;
        }
        Some((Value::String(_), _)) => {
            write_error(out, "WRONGTYPE Operation against a key holding the wrong kind of value")?;
        }
        None => {
            // List doesn't exist → length 0
            write!(out, ":0\r\n")?;
        }
    }

    Ok(())
}

pub fn cmd_lpop(
    out: &mut TcpStream,
    args: &[String],
    ctx: &Context,
) -> io::Result<()> {
    if args.len() != 2 && args.len() != 3 {
        write_error(out, "usage: LPOP <key> [count]")?;
        return Ok(());
    }

    let key = &args[1];
    let count = if args.len() == 3 {
        match args[2].parse::<usize>() {
            Ok(n) if n > 0 => Some(n),
            _ => {
                write_error(out, "ERR count must be a positive integer")?;
                return Ok(());
            }
        }
    } else {
        None
    };

    let mut map = ctx.store.lock().unwrap();

    match map.get_mut(key) {
        Some((Value::List(ref mut list), _)) => {
            if list.is_empty() {
                match count {
                    Some(_) => write!(out, "*0\r\n")?,     // empty array
                    None    => write!(out, "$-1\r\n")?,     // null bulk string
                }
                return Ok(());
            }

            match count {
                Some(n) => {
                    let actual_n = n.min(list.len());
                    let mut removed = Vec::with_capacity(actual_n);
                    for _ in 0..actual_n {
                        removed.push(list.remove(0));
                    }

                    write!(out, "*{}\r\n", removed.len())?;
                    for item in removed {
                        write!(out, "${}\r\n{}\r\n", item.len(), item)?;
                    }
                }
                None => {
                    let popped = list.remove(0);
                    write!(out, "${}\r\n{}\r\n", popped.len(), popped)?;
                }
            }
        }
        Some((Value::String(_), _)) => {
            write_error(out, "WRONGTYPE Operation against a key holding the wrong kind of value")?;
        }
        None => {
            match count {
                Some(_) => write!(out, "*0\r\n")?,
                None    => write!(out, "$-1\r\n")?,
            }
        }
    }

    Ok(())
}

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

pub fn cmd_type(
    out: &mut TcpStream,
    args: &[String],
    ctx: &Context,
) -> io::Result<()> {
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
                        // others to be added later
                    }
                }
            } else {
                match val {
                    Value::String(_) => "string",
                    Value::List(_) => "list",
                    // others to be added later
                }
            }
        }
        None => "none",
    };

    write_simple_string(out, response)
}
