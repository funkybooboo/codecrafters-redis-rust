// -----------------------------------------------------------------------------
// A minimal Redis clone implementing a subset of the RESP (REdis Serialization Protocol)
//  
// RESP supports five data types:
//  
// 1) Simple Strings: start with '+' and end with "\r\n".
//    Used for success replies like "+OK\r\n" or "+PONG\r\n".
//  
// 2) Errors: start with '-' and end with "\r\n".
//    Used for errors, e.g., "-ERR unknown command\r\n".
//  
// 3) Integers: start with ':' and end with "\r\n".
//    Used for integer replies (not used in this example).
//  
// 4) Bulk Strings: start with '$', then the byte length, then "\r\n",
//    then the data, then "\r\n". Example: "$3\r\nfoo\r\n".
//    A special length of -1 ("$-1\r\n") represents a Null Bulk String.
//  
// 5) Arrays: start with '*', then the number of elements, then "\r\n",
//    followed by that many Bulk Strings. Example: "*2\r\n$4\r\nPING\r\n$4\r\nECHO\r\n".
//  
// In this server, we parse Arrays of Bulk Strings as incoming commands,
// then respond with Simple Strings or Bulk Strings as appropriate.
// -----------------------------------------------------------------------------


use std::collections::HashMap;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

/// Read exactly one RESP Array from `reader` and return its elements as Rust `String`s.
/// Returns `Ok(None)` on EOF (client closed connection).
fn read_resp_array<R: BufRead>(reader: &mut R) -> io::Result<Option<Vec<String>>> {
    // 1) Read the array header line, e.g. "*2\r\n"
    let mut header = String::new();
    let bytes = reader.read_line(&mut header)?;
    if bytes == 0 {
        return Ok(None);
    }
    let header = header.trim_end_matches("\r\n");
    if !header.starts_with('*') {
        // Not an Array; ignore
        return Ok(Some(Vec::new()));
    }

    // Parse number of elements
    let count: usize = header[1..]
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid array header"))?;

    let mut args = Vec::with_capacity(count);
    // 2) Read each Bulk String argument
    for _ in 0..count {
        // Bulk header: "$<len>\r\n"
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

        // Read `len` bytes of data + CRLF
        let mut buf = vec![0; len + 2];
        reader.read_exact(&mut buf)?;

        // Strip trailing "\r\n" and convert
        let arg = String::from_utf8(buf[..len].to_vec())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid UTF-8"))?;
        args.push(arg);
    }

    Ok(Some(args))
}

/// Handle one client connection:
/// - Loop parsing RESP commands
/// - Dispatch PING, ECHO, SET, GET
/// - Reply with proper RESP types
fn handle_client(
    stream: TcpStream,
    store: Arc<Mutex<HashMap<String, String>>>,
) -> io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = stream;

    while let Some(args) = read_resp_array(&mut reader)? {
        if args.is_empty() {
            continue; // malformed or unrecognized frame
        }

        // Commands are case-insensitive
        let cmd = args[0].to_uppercase();
        match cmd.as_str() {
            "PING" => {
                // RESP Simple String: +PONG\r\n
                writer.write_all(b"+PONG\r\n")?;
            }
            "ECHO" => {
                // RESP Bulk String reply: $<len>\r\n<payload>\r\n
                if args.len() == 2 {
                    let payload = &args[1];
                    write!(writer, "${}\r\n", payload.len())?;
                    writer.write_all(payload.as_bytes())?;
                    writer.write_all(b"\r\n")?;
                } else {
                    writer.write_all(b"-ERR wrong number of arguments for 'echo'\r\n")?;
                }
            }
            "SET" => {
                // SET key value -> store and reply +OK\r\n
                if args.len() == 3 {
                    let key = args[1].clone();
                    let val = args[2].clone();
                    let mut map = store.lock().unwrap();
                    map.insert(key, val);
                    writer.write_all(b"+OK\r\n")?;
                } else {
                    writer.write_all(b"-ERR wrong number of arguments for 'set'\r\n")?;
                }
            }
            "GET" => {
                // GET key -> reply Bulk String or Null Bulk String
                if args.len() == 2 {
                    let key = &args[1];
                    let map = store.lock().unwrap();
                    if let Some(val) = map.get(key) {
                        write!(writer, "${}\r\n", val.len())?;
                        writer.write_all(val.as_bytes())?;
                        writer.write_all(b"\r\n")?;
                    } else {
                        // Null Bulk String for missing key
                        writer.write_all(b"$-1\r\n")?;
                    }
                } else {
                    writer.write_all(b"-ERR wrong number of arguments for 'get'\r\n")?;
                }
            }
            _ => {
                writer.write_all(b"-ERR unknown command\r\n")?;
            }
        }

        writer.flush()?;
    }

    Ok(())
}

fn main() -> io::Result<()> {
    let addr = "127.0.0.1:6379";
    let listener = TcpListener::bind(addr)?;
    println!("Listening on {}…", addr);

    // Shared in-memory store for all clients
    let store = Arc::new(Mutex::new(HashMap::new()));

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let store = Arc::clone(&store);
                thread::spawn(move || {
                    if let Err(err) = handle_client(stream, store) {
                        eprintln!("Client error: {}", err);
                    }
                });
            }
            Err(err) => eprintln!("Accept error: {}", err),
        }
    }

    Ok(())
}
