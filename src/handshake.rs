use std::io;
use std::net::TcpStream;
use crate::config::ServerConfig;
use crate::resp::write_resp_array;

/// Part 1 of the replica handshake: connect & send PING
pub fn replica_handshake(cfg: &ServerConfig) -> io::Result<()> {
    // 1) connect to the master
    let mut master = TcpStream::connect((&cfg.master_host[..], cfg.master_port))?;
    // 2) send ["PING"] as a RESP Array
    write_resp_array(&mut master, &["PING"])?;
    Ok(())
}
