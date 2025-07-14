use std::io;
use std::net::TcpStream;
use crate::config::ServerConfig;
use crate::resp::write_array;
use crate::utils::wait_for_it;

/// Part 1 & 2 of the replica handshake:
/// 1) PING
/// 2) wait_for_it -> +PONG
/// 3) REPLCONF listening-port <port>
/// 4) wait_for_it -> +OK
/// 5) REPLCONF capa psync2
/// 6) wait_for_it -> +OK
pub fn replica_handshake(cfg: &ServerConfig) -> io::Result<()> {
    // 1) connect
    let mut master = TcpStream::connect((&cfg.master_host[..], cfg.master_port))?;

    // 2) PING
    write_array(&mut master, &["PING"])?;
    // 3) wait for +PONG
    let _ = wait_for_it(&mut master)?;

    // 4) listening-port
    write_array(
        &mut master,
        &["REPLCONF", "listening-port", &cfg.port.to_string()],
    )?;
    // 5) wait for +OK
    let _ = wait_for_it(&mut master)?;

    // 6) capa psync2
    write_array(&mut master, &["REPLCONF", "capa", "psync2"])?;
    // 7) wait for +OK
    let _ = wait_for_it(&mut master)?;

    Ok(())
}
