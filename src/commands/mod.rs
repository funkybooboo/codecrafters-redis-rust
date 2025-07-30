mod keys;
mod replconf;
mod psync;
mod rpush;
mod lrange;
mod lpush;
mod llen;
mod lpop;
mod blpop;
mod typee;
mod xadd;
mod xrange;
mod xread;
mod incr;
mod ping;
mod echo;
pub mod set;
mod get;
mod config;
mod info;

use std::collections::HashMap;
use std::io;
use std::net::TcpStream;

use crate::Context;
use crate::commands::blpop::cmd_blpop;
use crate::commands::echo::cmd_echo;
use crate::commands::ping::cmd_ping;
use crate::commands::set::cmd_set;
use crate::commands::config::cmd_config;
use crate::commands::get::cmd_get;
use crate::commands::incr::cmd_incr;
use crate::commands::info::cmd_info;
use crate::commands::keys::cmd_keys;
use crate::commands::llen::cmd_llen;
use crate::commands::lpop::cmd_lpop;
use crate::commands::lpush::cmd_lpush;
use crate::commands::lrange::cmd_lrange;
use crate::commands::psync::cmd_psync;
use crate::commands::replconf::cmd_replconf;
use crate::commands::rpush::cmd_rpush;
use crate::commands::typee::cmd_type;
use crate::commands::xadd::cmd_xadd;
use crate::commands::xrange::cmd_xrange;
use crate::commands::xread::cmd_xread;

/// Every command has this signature
pub type CmdFn = fn(stream: &mut TcpStream, args: &[String], ctx: &Context) -> io::Result<()>;

// helper to know which commands should be fanned out
pub fn is_write_cmd(cmd: &str) -> bool {
    matches!(
        cmd,
        "SET"   // set a string
      | "DEL"   // delete a key
      | "RPUSH" // append to list
      | "LPUSH" // prepend to list
      | "LPOP"  // pop from head of list
      | "INCR"  // increment a numeric string
      | "XADD"  // append to a stream
    )
}

pub fn make_registry() -> HashMap<String, CmdFn> {
    let mut m = HashMap::new();
    m.insert("PING".into(),    cmd_ping   as CmdFn);    // respond with “PONG”
    m.insert("ECHO".into(),    cmd_echo   as CmdFn);    // send back whatever message you give
    m.insert("SET".into(),     cmd_set    as CmdFn);    // set a key to a value (optional expiry)
    m.insert("GET".into(),     cmd_get    as CmdFn);    // retrieve the value of a key
    m.insert("CONFIG".into(),  cmd_config as CmdFn);    // get a server configuration setting
    m.insert("KEYS".into(),    cmd_keys   as CmdFn);    // list all keys matching “*”
    m.insert("INFO".into(),    cmd_info   as CmdFn);    // show replication status info
    m.insert("REPLCONF".into(), cmd_replconf as CmdFn); // accept replication configuration from replica
    m.insert("PSYNC".into(),   cmd_psync  as CmdFn);    // perform partial resynchronization for replicas
    m.insert("RPUSH".into(),   cmd_rpush  as CmdFn);    // add one or more elements to the end of a list
    m.insert("LRANGE".into(),  cmd_lrange as CmdFn);    // return a slice of elements from a list
    m.insert("LPUSH".into(),   cmd_lpush  as CmdFn);    // add one or more elements to the front of a list
    m.insert("LLEN".into(),    cmd_llen   as CmdFn);    // get the number of elements in a list
    m.insert("LPOP".into(),    cmd_lpop   as CmdFn);    // remove and return element(s) from the front of a list
    m.insert("BLPOP".into(),   cmd_blpop  as CmdFn);    // block until an element is available, then pop from list
    m.insert("TYPE".into(),    cmd_type   as CmdFn);    // report the data type stored at a key
    m.insert("XADD".into(),    cmd_xadd   as CmdFn);    // append a new entry to a stream
    m.insert("XRANGE".into(),  cmd_xrange as CmdFn);    // read a range of entries from a stream
    m.insert("XREAD".into(),   cmd_xread  as CmdFn);    // read new stream entries, optionally waiting for them
    m.insert("INCR".into(),    cmd_incr   as CmdFn);    // increment the integer value of a key by one
    m
}
