use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

/// Read exactly one RESP Array from `reader` and return its elements as Rust `String`s.
/// Returns `Ok(None)` on EOF (client closed connection).
fn read_resp_array<R: BufRead>(reader: &mut R) -> io::Result<Option<Vec<String>>> {
    // 1) Read the array header line, e.g. "*2\r\n"
    //    "*" indicates an Array, "2" means two elements follow.
    let mut header = String::new();
    let bytes = reader.read_line(&mut header)?;
    if bytes == 0 {
        // EOF
        return Ok(None);
    }
    let header = header.trim_end_matches("\r\n");
    if !header.starts_with('*') {
        // Not a valid Array header: skip or ignore.
        return Ok(Some(Vec::new()));
    }

    // Parse the number of elements
    let count: usize = header[1..]
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid array header"))?;

    let mut args = Vec::with_capacity(count);

    // 2) For each element, read a Bulk String:
    for _ in 0..count {
        // 2a) Read bulk-string header, e.g. "$4\r\n"
        //     "$" indicates Bulk String, "4" is the byte-length of the payload.
        let mut bulk_header = String::new();
        reader.read_line(&mut bulk_header)?;
        let bulk_header = bulk_header.trim_end_matches("\r\n");
        if !bulk_header.starts_with('$') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Expected bulk string header",
            ));
        }
        let len: usize = bulk_header[1..]
            .parse()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid bulk length"))?;

        // 2b) Read exactly `len` bytes of data + 2 bytes for "\r\n"
        let mut buf = vec![0; len + 2];
        reader.read_exact(&mut buf)?;

        // Strip the trailing "\r\n" and convert to UTF-8
        let arg = String::from_utf8(buf[..len].to_vec())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid UTF-8"))?;
        args.push(arg);
    }

    Ok(Some(args))
}

/// Handle a single client connection:
/// - Repeatedly parse RESP commands with `read_resp_array`
/// - Dispatch on the command name (case-insensitive)
/// - Write back RESP-formatted replies
fn handle_client(stream: TcpStream) -> io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = stream;

    // Loop until client disconnects
    while let Some(args) = read_resp_array(&mut reader)? {
        if args.is_empty() {
            // Malformed or unsupported frame—ignore and continue
            continue;
        }

        // Redis commands are case-insensitive
        let cmd = args[0].to_uppercase();
        match cmd.as_str() {
            "PING" => {
                // Simple-string reply: "+PONG\r\n"
                // RESP Simple String: starts with '+', then message, then CRLF
                writer.write_all(b"+PONG\r\n")?;
            }
            "ECHO" => {
                // Bulk-string reply: first "$<len>\r\n", then data, then "\r\n"
                if args.len() == 2 {
                    let payload = &args[1];
                    // Write bulk header with payload length
                    write!(writer, "${}\r\n", payload.len())?;
                    // Write the payload bytes
                    writer.write_all(payload.as_bytes())?;
                    // Terminate with CRLF
                    writer.write_all(b"\r\n")?;
                } else {
                    // Wrong number of arguments: RESP Error
                    // RESP Error: starts with '-', then message, then CRLF
                    writer.write_all(b"-ERR wrong number of arguments for 'echo' command\r\n")?;
                }
            }
            _ => {
                // Unknown command: RESP Error
                writer.write_all(b"-ERR unknown command\r\n")?;
            }
        }

        // Flush to ensure the reply is sent immediately
        writer.flush()?;
    }

    Ok(())
}

fn main() -> io::Result<()> {
    let addr = "127.0.0.1:6379";
    let listener = TcpListener::bind(addr)?;
    println!("Listening on {}…", addr);

    // Accept connections in a loop, spawning a new thread per client
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
