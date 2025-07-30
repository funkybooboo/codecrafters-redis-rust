use std::io;
use std::io::{BufRead, BufReader};
use std::net::TcpStream;
use crate::commands::set::cmd_set;
use crate::config::ServerConfig;
use crate::Context;
use crate::resp::read_array;
use crate::resp::write_array;

/// Full replica handshake (parts 1–3):
///
/// 1) PING
///    -> +PONG
/// 2) REPLCONF listening-port <our-port>
///    -> +OK
/// 3) REPLCONF capa psync2
///    -> +OK
/// 4) PSYNC ? -1
///    -> +FULLRESYNC <replid> 0
pub fn replica_handshake(cfg: &ServerConfig) -> io::Result<TcpStream> {
    // Connect to the master
    let mut master = TcpStream::connect((&cfg.master_host[..], cfg.master_port))?;

    // 1) Send PING
    write_array(&mut master, &["PING"])?;
    // Wait for +PONG
    let _ = wait_for_it(&mut master)?;

    // 2) Notify listening port
    write_array(&mut master, &["REPLCONF", "listening-port", &cfg.port.to_string()])?;
    // Wait for +OK
    let _ = wait_for_it(&mut master)?;

    // 3) Send capabilities
    write_array(&mut master, &["REPLCONF", "capa", "psync2"])?;
    // Wait for +OK
    let _ = wait_for_it(&mut master)?;

    // 4) PSYNC initial sync
    write_array(&mut master, &["PSYNC", "?", "-1"])?;
    // Wait for +FULLRESYNC …\r\n (we'll parse it later)
    let _ = wait_for_it(&mut master)?;

    Ok(master)
}

/// Reads exactly one line (`+\r\n`, `-ERR…\r\n`, etc.) from the master
/// and returns it (including the trailing CRLF).
fn wait_for_it(stream: &mut TcpStream) -> io::Result<String> {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    Ok(line)
}

/// Read commands from master, replay only write‐type ones through your normal cmd_*
/// (which mutates ctx.store for you). Any “OK” they emit is ignored.
pub fn replication_loop(
    stream: TcpStream,
    ctx: Context,
) -> io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = stream;

    while let Some(args) = read_array(&mut reader)? {
        if args.is_empty() { continue; }
        match args[0].to_uppercase().as_str() {
            "SET" => {
                // re‐use your regular cmd_set implementation
                let _ = cmd_set(&mut writer, &args, &ctx);
            }
            // add other write‐type commands here…
            _ => {}
        }
    }

    Ok(())
}
