use std::io;
use std::io::{BufRead, BufReader};
use std::net::TcpStream;

/// Reads exactly one line (`+\r\n`, `-ERR…\r\n`, etc.) from the master
/// and returns it (including the trailing CRLF).
pub fn wait_for_it(stream: &mut TcpStream) -> io::Result<String> {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    Ok(line)
}
