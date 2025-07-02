use std::io::{self, BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

/// Handle a single client:
/// - Read line by line (buffered for you).
/// - Trim trailing CR/LF and if the content is exactly "PING",
///   write back "+PONG\r\n".
/// - Loop until EOF.
fn handle_client(stream: TcpStream) -> io::Result<()> {
    // Clone the stream so we can both read lines and write replies
    let reader = BufReader::new(stream.try_clone()?);
    let mut writer = stream;

    for line in reader.lines() {
        let line = line?;                      // strips trailing '\n'
        if line.trim_end_matches('\r') == "PING" {
            writer.write_all(b"+PONG\r\n")?;
        }
    }

    Ok(())
}

fn main() -> io::Result<()> {
    let addr = "127.0.0.1:6379";
    let listener = TcpListener::bind(addr)?;
    println!("Listening on {}…", addr);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(|| {
                    if let Err(err) = handle_client(stream) {
                        eprintln!("Client error: {}", err);
                    }
                });
            }
            Err(err) => eprintln!("Accept error: {}", err),
        }
    }

    Ok(())
}
