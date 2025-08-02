mod admin;
mod connection;
mod list;
mod replication;
mod stream;
mod string;
mod transaction;

use lazy_static::lazy_static;
use std::{collections::HashMap, io};
use std::io::Write;
use std::net::TcpStream;
use crate::commands::admin::config::cmd_config;
use crate::commands::admin::info::cmd_info;
use crate::commands::admin::keys::cmd_keys;
use crate::commands::connection::echo::cmd_echo;
use crate::commands::connection::ping::cmd_ping;
use crate::commands::list::blpop::cmd_blpop;
use crate::commands::list::llen::cmd_llen;
use crate::commands::list::lpop::cmd_lpop;
use crate::commands::list::lpush::cmd_lpush;
use crate::commands::list::lrange::cmd_lrange;
use crate::commands::list::rpush::cmd_rpush;
use crate::commands::replication::psync::cmd_psync;
use crate::commands::replication::replconf::cmd_replconf;
use crate::commands::stream::xadd::cmd_xadd;
use crate::commands::stream::xrange::cmd_xrange;
use crate::commands::stream::xread::cmd_xread;
use crate::commands::string::get::cmd_get;
use crate::commands::string::incr::cmd_incr;
use crate::commands::string::set::cmd_set;
use crate::commands::string::typee::cmd_type;
use crate::commands::transaction::discard::cmd_discard;
use crate::commands::transaction::exec::cmd_exec;
use crate::commands::transaction::multi::cmd_multi;
use crate::resp::write_resp_error;
use crate::Context;
use crate::role::Role;

pub type CmdFn = fn(&[String], &mut Context) -> io::Result<Vec<u8>>;

lazy_static! {
    /// Full map of *all* commands
    static ref ALL_CMDS: HashMap<String, CmdFn> = {
        let mut m = HashMap::new();
        m.insert("PING".into(),     cmd_ping    as CmdFn);   // always reply with “PONG”
        m.insert("ECHO".into(),     cmd_echo    as CmdFn);   // send back whatever text you give
        m.insert("SET".into(),      cmd_set     as CmdFn);   // set a key to a value (with optional expiry)
        m.insert("GET".into(),      cmd_get     as CmdFn);   // retrieve the value stored at a key
        m.insert("CONFIG".into(),   cmd_config  as CmdFn);   // fetch a server configuration setting
        m.insert("KEYS".into(),     cmd_keys    as CmdFn);   // list all keys matching a pattern
        m.insert("INFO".into(),     cmd_info    as CmdFn);   // return server info (e.g. replication status)
        m.insert("REPLCONF".into(), cmd_replconf as CmdFn);  // accept replication options from a replica
        m.insert("PSYNC".into(),    cmd_psync   as CmdFn);   // perform partial resynchronization for replicas
        m.insert("RPUSH".into(),    cmd_rpush   as CmdFn);   // append one or more elements to the end of a list
        m.insert("LRANGE".into(),   cmd_lrange  as CmdFn);   // get a subrange of elements from a list
        m.insert("LPUSH".into(),    cmd_lpush   as CmdFn);   // prepend one or more elements to a list
        m.insert("LLEN".into(),     cmd_llen    as CmdFn);   // return the number of elements in a list
        m.insert("LPOP".into(),     cmd_lpop    as CmdFn);   // remove and return element(s) from the front of a list
        m.insert("BLPOP".into(),    cmd_blpop   as CmdFn);   // block until an element is available, then pop it
        m.insert("TYPE".into(),     cmd_type    as CmdFn);   // report the data type stored at a key
        m.insert("XADD".into(),     cmd_xadd    as CmdFn);   // append a new entry to a stream
        m.insert("XRANGE".into(),   cmd_xrange  as CmdFn);   // read a range of entries from a stream
        m.insert("XREAD".into(),    cmd_xread   as CmdFn);   // read from one or more streams (optionally blocking)
        m.insert("INCR".into(),     cmd_incr    as CmdFn);   // increment the integer value of a key by one
        m.insert("MULTI".into(),    cmd_multi   as CmdFn);   // start a transaction
        m.insert("EXEC".into(),     cmd_exec    as CmdFn);   // run queued commands (error if no MULTI)
        m.insert("DISCARD".into(),  cmd_discard as CmdFn);   // abort the current transaction and clear all queued commands
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
    let result = matches!(
        cmd,
        "SET" | "DEL" | "RPUSH" | "LPUSH" | "LPOP" | "INCR" | "XADD"
    );
    println!("[commands::is_write_cmd] '{}' is write cmd: {}", cmd, result);
    result
}

/// Dispatch any command (used by your normal server loop).
/// Emits “unknown command” on unrecognized names.
pub fn dispatch_cmd(
    name: &str,
    out: &mut TcpStream,
    args: &[String],
    ctx: &mut Context,
) -> io::Result<()> {
    println!("[commands::dispatch_cmd] Dispatching command: '{}'", name);

    if let Some(cmd_fn) = ALL_CMDS.get(name) {
        println!("[commands::dispatch_cmd] Found handler. Executing...");

        let response = cmd_fn(args, ctx)?;

        if ctx.cfg.role == Role::Slave {
            println!("[commands::dispatch_cmd] Replica mode: response suppressed for '{}'", name);
        } else {
            println!("[commands::dispatch_cmd] Sending response ({} bytes)", response.len());
            out.write_all(&response)?;
        }

        Ok(())
    } else {
        eprintln!("[commands::dispatch_cmd] Unknown command: '{}'", name);
        write_resp_error(out, "unknown command")?;
        Ok(())
    }
}

/// Replay only write‐type commands (used by replication).
/// Silently ignores everything else.
pub fn replay_cmd(
    name: &str,
    _out: &mut TcpStream, // no longer used
    args: &[String],
    ctx: &mut Context,
) -> io::Result<()> {
    println!("[commands::replay_cmd] Attempting to replay command: '{}'", name);

    if let Some(cmd_fn) = WRITE_CMDS.get(name) {
        println!("[commands::replay_cmd] Replaying write command: '{}'", name);
        let _ = cmd_fn(args, ctx); // execute for side effect only, ignore output
    } else {
        println!("[commands::replay_cmd] Ignored non-write command: '{}'", name);
    }

    Ok(())
}
