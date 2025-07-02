use std::io::{Read, Write, Result};
use std::net::{TcpListener, TcpStream};
use std::thread;

/// Handles one client connection:
/// - Reads up to 512 bytes at a time into `buf`.
/// - Buffers across reads in `pending`.
/// - Whenever it sees a full line ending in `\n`, it checks:
///     • If the line’s content (minus any trailing `\r`) is exactly `"PING"`,
///       it writes back `+PONG\r\n`.
///     • Otherwise, it ignores that line (this skips RESP framing or any
///       other noise).
/// - Leaves any incomplete line in `pending` for the next read.
/// - Loops until the client closes the connection.
fn handle_client(mut stream: TcpStream) -> Result<()> {
    let mut buf = [0u8; 512];   // scratch space for each read
    let mut pending = Vec::new(); // accumulated bytes between reads

    loop {
        // 1) Read from the socket
        let n = stream.read(&mut buf)?;
        if n == 0 {
            // Client closed connection
            break;
        }
        // 2) Append newly read bytes
        pending.extend_from_slice(&buf[..n]);

        // 3) Scan `pending` for newline-terminated lines
        let mut processed_up_to = 0;
        for i in 0..pending.len() {
            if pending[i] == b'\n' {
                // Slice out the full line, including the newline
                let mut line = &pending[processed_up_to..=i];

                // Strip off trailing "\r\n" or "\n"
                if line.ends_with(&[b'\r', b'\n']) {
                    line = &line[..line.len() - 2];
                } else if line.ends_with(&[b'\n']) {
                    line = &line[..line.len() - 1];
                }

                // If the client literally sent "PING", reply once
                if line == b"PING" {
                    stream.write_all(b"+PONG\r\n")?;
                }

                // Mark that we’ve handled through byte `i`
                processed_up_to = i + 1;
            }
        }

        // 4) Drop all the bytes we’ve processed, keep the rest
        if processed_up_to > 0 {
            pending.drain(0..processed_up_to);
        }
    }

    Ok(())
}

fn main() -> Result<()> {
    let addr = "127.0.0.1:6379";
    let listener = TcpListener::bind(addr)?;
    println!("Listening on {}…", addr);

    // Accept each new connection and immediately hand it off to a new thread
    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                // spawn a thread to handle this client
                thread::spawn(move || {
                    if let Err(e) = handle_client(stream) {
                        eprintln!("Client error: {}", e);
                    }
                });
            }
            Err(e) => eprintln!("Accept error: {}", e),
        }
    }

    Ok(())
}
