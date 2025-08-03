mod admin;
mod connection;
mod list;
mod replication;
mod stream;
mod string;
mod transaction;
mod pubsub;

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
use crate::commands::pubsub::publish::cmd_publish;
use crate::commands::pubsub::subscribe::cmd_subscribe;
use crate::commands::pubsub::unsubscribe::cmd_unsubscribe;
use crate::commands::replication::psync::cmd_psync;
use crate::commands::replication::replconf::cmd_replconf;
use crate::commands::replication::wait::cmd_wait;
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
        m.insert("PING".into(),     cmd_ping    as CmdFn);
        m.insert("ECHO".into(),     cmd_echo    as CmdFn);
        m.insert("SET".into(),      cmd_set     as CmdFn);
        m.insert("GET".into(),      cmd_get     as CmdFn);
        m.insert("CONFIG".into(),   cmd_config  as CmdFn);
        m.insert("KEYS".into(),     cmd_keys    as CmdFn);
        m.insert("INFO".into(),     cmd_info    as CmdFn);
        m.insert("REPLCONF".into(), cmd_replconf as CmdFn);
        m.insert("PSYNC".into(),    cmd_psync   as CmdFn);
        m.insert("RPUSH".into(),    cmd_rpush   as CmdFn);
        m.insert("LRANGE".into(),   cmd_lrange  as CmdFn);
        m.insert("LPUSH".into(),    cmd_lpush   as CmdFn);
        m.insert("LLEN".into(),     cmd_llen    as CmdFn);
        m.insert("LPOP".into(),     cmd_lpop    as CmdFn);
        m.insert("BLPOP".into(),    cmd_blpop   as CmdFn);
        m.insert("TYPE".into(),     cmd_type    as CmdFn);
        m.insert("XADD".into(),     cmd_xadd    as CmdFn);
        m.insert("XRANGE".into(),   cmd_xrange  as CmdFn);
        m.insert("XREAD".into(),    cmd_xread   as CmdFn);
        m.insert("INCR".into(),     cmd_incr    as CmdFn);
        m.insert("MULTI".into(),    cmd_multi   as CmdFn);
        m.insert("EXEC".into(),     cmd_exec    as CmdFn);
        m.insert("DISCARD".into(),  cmd_discard as CmdFn);
        m.insert("WAIT".into(),     cmd_wait    as CmdFn);
        m.insert("SUBSCRIBE".into(), cmd_subscribe as CmdFn);
        m.insert("PUBLISH".into(), cmd_publish as CmdFn);
        m.insert("UNSUBSCRIBE".into(), cmd_unsubscribe as CmdFn);
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

pub fn is_write_cmd(cmd: &str) -> bool {
    matches!(
        cmd.to_ascii_uppercase().as_str(),
        "SET" | "DEL" | "RPUSH" | "LPUSH" | "LPOP" | "INCR" | "XADD"
    )
}

/// Should a normal client get a reply?
fn should_respond(cmd: &str, ctx: &Context) -> bool {
    match ctx.cfg.role {
        Role::Master => true,
        Role::Slave  => !cmd.eq_ignore_ascii_case("REPLCONF"),
    }
}

/// Dispatches a command for either a client or the replication link.
/// - **Clients**: replies per `should_respond`.
/// - **Replication link** (the socket back to the master): only emits `REPLCONF` responses
///   (so you don’t echo `+PONG` for `PING`, etc.).
pub fn dispatch_cmd(
    name: &str,
    out: &mut TcpStream,
    args: &[String],
    ctx: &mut Context,
) -> io::Result<()> {
    println!("[dispatch_cmd] Dispatching command: '{}'", name);

    // Are we in replica mode, and is this socket the replication link back to the master?
    let is_repl_link = if ctx.cfg.role == Role::Slave {
        out.peer_addr()
            .map(|peer| peer.port() == ctx.cfg.master_port)
            .unwrap_or(false)
    } else {
        false
    };

    if let Some(cmd_fn) = ALL_CMDS.get(name) {
        // Execute for side‐effects (store update, offsets, etc.)
        let response = cmd_fn(args, ctx)?;

        if is_repl_link {
            // Swallow everything except REPLCONF
            if name.eq_ignore_ascii_case("REPLCONF") {
                println!("[dispatch_cmd] (repl link) sending REPLCONF reply");
                out.write_all(&response)?;
            } else {
                println!("[dispatch_cmd] (repl link) swallowing reply to '{}'", name);
            }
        } else {
            // Regular client: follow should_respond
            if should_respond(name, ctx) {
                println!("[dispatch_cmd] (client) sending reply to '{}'", name);
                out.write_all(&response)?;
            } else {
                println!("[dispatch_cmd] (client) skipping reply to '{}'", name);
            }
        }

        Ok(())
    } else {
        eprintln!("[dispatch_cmd] Unknown command: '{}'", name);
        // Only error out to real clients
        if !is_repl_link {
            write_resp_error(out, "unknown command")?;
        }
        Ok(())
    }
}
