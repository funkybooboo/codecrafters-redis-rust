use std::io;
use std::net::TcpStream;
use crate::config::ServerConfig;
use crate::resp::write_array;
use crate::utils::wait_for_it;

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
