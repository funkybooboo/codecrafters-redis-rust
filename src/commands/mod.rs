mod blpop;
mod config;
mod echo;
mod get;
mod incr;
mod info;
mod keys;
mod llen;
mod lpop;
mod lpush;
mod lrange;
mod ping;
mod psync;
mod replconf;
mod rpush;
mod set;
mod typee;
mod xadd;
mod xrange;
mod xread;

use lazy_static::lazy_static;
use std::{collections::HashMap, io, net::TcpStream};

use crate::resp::write_error;
use crate::Context;

// bring in each cmd_xxx
use self::blpop::cmd_blpop;
use self::config::cmd_config;
use self::echo::cmd_echo;
use self::get::cmd_get;
use self::incr::cmd_incr;
use self::info::cmd_info;
use self::keys::cmd_keys;
use self::llen::cmd_llen;
use self::lpop::cmd_lpop;
use self::lpush::cmd_lpush;
use self::lrange::cmd_lrange;
use self::ping::cmd_ping;
use self::psync::cmd_psync;
use self::replconf::cmd_replconf;
use self::rpush::cmd_rpush;
use self::set::cmd_set;
use self::typee::cmd_type;
use self::xadd::cmd_xadd;
use self::xrange::cmd_xrange;
use self::xread::cmd_xread;

/// Every command has this signature
pub type CmdFn = fn(&mut TcpStream, &[String], &Context) -> io::Result<()>;

lazy_static! {
    /// Full map of *all* commands
    static ref ALL_CMDS: HashMap<String, CmdFn> = {
        let mut m = HashMap::new();
        m.insert("PING".into(),    cmd_ping   as CmdFn);
        m.insert("ECHO".into(),    cmd_echo   as CmdFn);
        m.insert("SET".into(),     cmd_set    as CmdFn);
        m.insert("GET".into(),     cmd_get    as CmdFn);
        m.insert("CONFIG".into(),  cmd_config as CmdFn);
        m.insert("KEYS".into(),    cmd_keys   as CmdFn);
        m.insert("INFO".into(),    cmd_info   as CmdFn);
        m.insert("REPLCONF".into(), cmd_replconf as CmdFn);
        m.insert("PSYNC".into(),   cmd_psync  as CmdFn);
        m.insert("RPUSH".into(),   cmd_rpush  as CmdFn);
        m.insert("LRANGE".into(),  cmd_lrange as CmdFn);
        m.insert("LPUSH".into(),   cmd_lpush  as CmdFn);
        m.insert("LLEN".into(),    cmd_llen   as CmdFn);
        m.insert("LPOP".into(),    cmd_lpop   as CmdFn);
        m.insert("BLPOP".into(),   cmd_blpop  as CmdFn);
        m.insert("TYPE".into(),    cmd_type   as CmdFn);
        m.insert("XADD".into(),    cmd_xadd   as CmdFn);
        m.insert("XRANGE".into(),  cmd_xrange as CmdFn);
        m.insert("XREAD".into(),   cmd_xread  as CmdFn);
        m.insert("INCR".into(),    cmd_incr   as CmdFn);
        m
    };

    /// Pre‐filtered map of only the write‐type commands
    static ref WRITE_CMDS: HashMap<String, CmdFn> = {
        ALL_CMDS
            .iter()
            .filter(|(name, _)| is_write_cmd(name))
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    };
}

/// Which commands actually mutate state
pub fn is_write_cmd(cmd: &str) -> bool {
    matches!(
        cmd,
        "SET"   // set a string
      | "DEL"   // delete a key
      | "RPUSH" // append to list
      | "LPUSH" // prepend to list
      | "LPOP"  // pop from head of list
      | "INCR"  // increment a numeric string
      | "XADD" // append to a stream
    )
}

/// Dispatch any command (used by your normal server loop).
/// Emits “unknown command” on unrecognized names.
pub fn dispatch_cmd(
    name: &str,
    out: &mut TcpStream,
    args: &[String],
    ctx: &Context,
) -> io::Result<()> {
    if let Some(cmd_fn) = ALL_CMDS.get(name) {
        cmd_fn(out, args, ctx)
    } else {
        write_error(out, "unknown command")?;
        Ok(())
    }
}

/// Replay only write‐type commands (used by replication).
/// Silently ignores everything else.
pub fn replay_cmd(
    name: &str,
    out: &mut TcpStream,
    args: &[String],
    ctx: &Context,
) -> io::Result<()> {
    if let Some(cmd_fn) = WRITE_CMDS.get(name) {
        let _ = cmd_fn(out, args, ctx);
    }
    Ok(())
}
