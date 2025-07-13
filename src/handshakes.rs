use std::io;
use std::io::{BufRead, BufReader};
use std::net::TcpStream;
use crate::config::ServerConfig;
use crate::resp::write_resp_array;

/// Part 1 & 2 of the replica handshake:
///  1) PING
///  2) read +PONG
///  3) REPLCONF listening-port <our-port>
///  4) read +OK
///  5) REPLCONF capa psync2
///  6) read +OK
pub fn replica_handshake(cfg: &ServerConfig) -> io::Result<()> {
    // connect to the master
    let mut stream = TcpStream::connect((cfg.master_host.as_str(), cfg.master_port))?;

    // 1) send PING
    write_resp_array(&mut stream, &["PING"])?;
    // 2) wait for +PONG\r\n
    {
        let mut reader = BufReader::new(&mut stream);
        let mut line = String::new();
        reader.read_line(&mut line)?;  // should be "+PONG\r\n"
    }

    // 3) notify listening port
    write_resp_array(
        &mut stream,
        &["REPLCONF", "listening-port", &cfg.port.to_string()],
    )?;
    // 4) wait for +OK\r\n
    {
        let mut reader = BufReader::new(&mut stream);
        let mut line = String::new();
        reader.read_line(&mut line)?;
    }

    // 5) advertise psync2 capability
    write_resp_array(&mut stream, &["REPLCONF", "capa", "psync2"])?;
    // 6) wait for +OK\r\n
    {
        let mut reader = BufReader::new(&mut stream);
        let mut line = String::new();
        reader.read_line(&mut line)?;
    }

    Ok(())
}
