use std::{
    collections::HashMap,
    io::{self, Write},
    sync::{Mutex},
};
use std::time::{Duration, SystemTime};
use crate::config::ServerConfig;
use crate::resp::write_bulk_string;

pub(crate) type Store = Mutex<HashMap<String, (String, Option<SystemTime>)>>;

/// PING -> +PONG
pub fn cmd_ping<W: Write>(out: &mut W) -> io::Result<()> {
    out.write_all(b"+PONG\r\n")
}

/// ECHO <msg> -> BulkString(msg)
pub fn cmd_echo<W: Write>(out: &mut W, args: &[String]) -> io::Result<()> {
    if args.len() == 2 {
        write_bulk_string(out, &args[1])
    } else {
        out.write_all(b"-ERR wrong number of arguments for 'echo'\r\n")
    }
}

/// SET key value [PX ms]
pub fn cmd_set<W: Write>(
    out: &mut W,
    args: &[String],
    store: &Store,
) -> io::Result<()> {
    match args.len() {
        3 => {
            let mut m = store.lock().unwrap();
            m.insert(args[1].clone(), (args[2].clone(), None));
            out.write_all(b"+OK\r\n")
        }
        5 if args[3].eq_ignore_ascii_case("PX") => {
            if let Ok(ms) = args[4].parse::<u64>() {
                // record a SystemTime expiry
                let expiry = SystemTime::now()
                    .checked_add(Duration::from_millis(ms))
                    .unwrap();
                let mut m = store.lock().unwrap();
                m.insert(args[1].clone(), (args[2].clone(), Some(expiry)));
                out.write_all(b"+OK\r\n")
            } else {
                out.write_all(b"-ERR invalid PX value\r\n")
            }
        }
        _ => out.write_all(b"-ERR wrong number of arguments for 'set'\r\n"),
    }
}

/// GET key -> BulkString or NullBulk if missing/expired
pub fn cmd_get<W: Write>(
    out: &mut W,
    args: &[String],
    store: &Store,
) -> io::Result<()> {
    if args.len() != 2 {
        return out.write_all(b"-ERR wrong number of arguments for 'get'\r\n");
    }
    let key = &args[1];
    let mut m = store.lock().unwrap();

    if let Some((val, opt_expiry)) = m.get(key).cloned() {
        // if there's an expiry, and it's passed, delete and return NULL
        if let Some(expiry) = opt_expiry {
            if SystemTime::now() >= expiry {
                m.remove(key);
                return out.write_all(b"$-1\r\n");
            }
        }
        write_bulk_string(out, &val)
    } else {
        out.write_all(b"$-1\r\n")
    }
}

/// CONFIG GET <dir|dbfilename>
pub fn cmd_config<W: Write>(
    out: &mut W,
    args: &[String],
    cfg: &ServerConfig,
) -> io::Result<()> {
    if args.len() == 3 && args[1].eq_ignore_ascii_case("GET") {
        let key = args[2].as_str();
        let value = match key {
            "dir" => &cfg.dir,
            "dbfilename" => &cfg.dbfilename,
            _ => return out.write_all(b"-ERR unknown config parameter\r\n"),
        };
        // RESP Array of two Bulk Strings
        write!(out, "*2\r\n")?;
        write_bulk_string(out, key)?;
        write_bulk_string(out, value)?;
        Ok(())
    } else {
        out.write_all(b"-ERR wrong number of arguments for 'CONFIG'\r\n")
    }
}

/// KEYS "*"
pub fn cmd_keys<W: Write>(
    out: &mut W,
    args: &[String],
    store: &Store,
) -> io::Result<()> {
    if args.len() != 2 || args[1] != "*" {
        return out.write_all(b"-ERR only '*' pattern supported\r\n");
    }
    let map = store.lock().unwrap();
    let mut keys: Vec<&String> = map.keys().collect();
    keys.sort(); // deterministic order
    write!(out, "*{}\r\n", keys.len())?;
    for key in keys {
        write_bulk_string(out, key)?;
    }
    Ok(())
}

/// INFO [section]
/// In this stage we only support the "replication" section
pub fn cmd_info<W: Write>(
    out: &mut W,
    args: &[String],
    cfg: &ServerConfig
) -> io::Result<()> {
    if args.len() == 2 && args[1].eq_ignore_ascii_case("replication") {
        // build one CRLF‐delimited string with all the fields:
        let info = format!(
            "role:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}",
            cfg.role,
            cfg.master_replid,
            cfg.master_repl_offset
        );
        // this single call will emit:
        // $<len>\r\nrole:master\r\nmaster_replid:<id>\r\nmaster_repl_offset:<offset>\r\n
        write_bulk_string(out, &info)
    } else {
        // unsupported section → empty bulk string
        write_bulk_string(out, "")
    }
}

/// REPLCONF <option> <value>
/// For now we just always reply +OK
pub fn cmd_replconf<W: Write>(out: &mut W, args: &[String]) -> io::Result<()> {
    if args.len() == 3 {
        // valid form: REPLCONF <opt> <val>
        out.write_all(b"+OK\r\n")
    } else {
        out.write_all(b"-ERR wrong number of arguments for 'REPLCONF'\r\n")
    }
}
