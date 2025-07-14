use std::collections::HashMap;
use std::io::{self, Write};
use std::time::{Duration, SystemTime};

use crate::config::ServerConfig;
use crate::resp::{
    write_simple_string, write_error, write_bulk_string, check_len,
};
use crate::rdb::{Store, EMPTY_RDB};

/// A little context bundling everything cmds might need
pub struct Context<'a> {
    pub cfg: &'a ServerConfig,
    pub store: &'a Store,
}

/// Every command has this signature
pub type CmdFn = fn(out: &mut dyn Write, args: &[String], ctx: &Context) -> io::Result<()>;

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
    m
}

/// PING -> +PONG
pub fn cmd_ping(
    out: &mut dyn Write,
    _args: &[String],
    _ctx: &Context,
) -> io::Result<()> {
    write_simple_string(out, "PONG")
}

/// ECHO <msg> -> BulkString(msg)
pub fn cmd_echo(
    out: &mut dyn Write,
    args: &[String],
    _ctx: &Context,
) -> io::Result<()> {
    if !check_len(out, args, 2, "usage: ECHO <msg>") {
        return Ok(());
    }
    write_bulk_string(out, &args[1])
}

/// SET key value [PX ms]
pub fn cmd_set(
    out: &mut dyn Write,
    args: &[String],
    ctx: &Context,
) -> io::Result<()> {
    if args.len() != 3 && args.len() != 5 {
        write_error(out, "usage: SET <key> <val> [PX ms]")?;
        return Ok(());
    }

    let key = &args[1];
    let val = &args[2];
    let mut map = ctx.store.lock().unwrap();

    if args.len() == 3 {
        map.insert(key.clone(), (val.clone(), None));
    } else {
        let ms = args[4].parse::<u64>().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "PX must be integer")
        })?;
        let expiry = SystemTime::now()
            .checked_add(Duration::from_millis(ms))
            .unwrap();
        map.insert(key.clone(), (val.clone(), Some(expiry)));
    }

    write_simple_string(out, "OK")
}

/// GET key -> BulkString or NullBulk
pub fn cmd_get(
    out: &mut dyn Write,
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
        write_bulk_string(out, &val)
    } else {
        out.write_all(b"$-1\r\n")
    }
}

/// CONFIG GET <dir|dbfilename>
pub fn cmd_config(
    out: &mut dyn Write,
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
    out.write_all(format!("*2\r\n").as_bytes())?;
    write_bulk_string(out, key)?;
    write_bulk_string(out, val)
}

/// KEYS "*"
pub fn cmd_keys(
    out: &mut dyn Write,
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
    out: &mut dyn Write,
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
    out: &mut dyn Write,
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
    out: &mut dyn Write,
    args: &[String],
    ctx: &Context,
) -> io::Result<()> {
    if !check_len(out, args, 3, "usage: PSYNC <master_replid> <master_repl_offset>") {
        return Ok(());
    }

    // 1) Send "+FULLRESYNC <id> 0\r\n"
    let full = format!("FULLRESYNC {} 0", ctx.cfg.master_replid);
    write_simple_string(out, &full)?;

    // 2) Send empty RDB length + raw bytes
    write!(out, "${}\r\n", EMPTY_RDB.len())?;
    out.write_all(EMPTY_RDB)?;

    Ok(())
}
