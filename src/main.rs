use std::collections::HashMap;
use std::env;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Holds our two RDB persistence settings.
#[derive(Debug)]
struct ServerConfig {
    dir: String,
    dbfilename: String,
}

/// Read one RESP Array of Bulk Strings from `reader`.
fn read_resp_array<R: BufRead>(reader: &mut R) -> io::Result<Option<Vec<String>>> {
    let mut header = String::new();
    if reader.read_line(&mut header)? == 0 {
        return Ok(None);
    }
    let header = header.trim_end_matches("\r\n");
    if !header.starts_with('*') {
        return Ok(Some(Vec::new()));
    }
    let count: usize = header[1..].parse().map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidData, "Invalid array header")
    })?;

    let mut args = Vec::with_capacity(count);
    for _ in 0..count {
        let mut bulk_header = String::new();
        reader.read_line(&mut bulk_header)?;
        let bulk_header = bulk_header.trim_end_matches("\r\n");
        if !bulk_header.starts_with('$') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Expected bulk string header",
            ));
        }
        let len: usize = bulk_header[1..].parse().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "Invalid bulk length")
        })?;

        let mut buf = vec![0; len + 2];
        reader.read_exact(&mut buf)?;
        let arg = String::from_utf8(buf[..len].to_vec()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "Invalid UTF-8")
        })?;
        args.push(arg);
    }

    Ok(Some(args))
}

fn handle_ping<W: Write>(writer: &mut W) -> io::Result<()> {
    writer.write_all(b"+PONG\r\n")
}

fn handle_echo<W: Write>(writer: &mut W, args: &[String]) -> io::Result<()> {
    if args.len() == 2 {
        let payload = &args[1];
        write!(writer, "${}\r\n", payload.len())?;
        writer.write_all(payload.as_bytes())?;
        writer.write_all(b"\r\n")
    } else {
        writer.write_all(b"-ERR wrong number of arguments for 'echo'\r\n")
    }
}

fn handle_set<W: Write>(
    writer: &mut W,
    args: &[String],
    store: &Mutex<HashMap<String, (String, Option<Instant>)>>,
) -> io::Result<()> {
    match args.len() {
        3 => {
            let key = args[1].clone();
            let val = args[2].clone();
            let mut map = store.lock().unwrap();
            map.insert(key, (val, None));
            writer.write_all(b"+OK\r\n")
        }
        5 if args[3].to_uppercase() == "PX" => {
            let key = args[1].clone();
            let val = args[2].clone();
            match args[4].parse::<u64>() {
                Ok(ms) => {
                    let expiry = Instant::now() + Duration::from_millis(ms);
                    let mut map = store.lock().unwrap();
                    map.insert(key, (val, Some(expiry)));
                    writer.write_all(b"+OK\r\n")
                }
                Err(_) => writer.write_all(b"-ERR invalid PX value\r\n"),
            }
        }
        _ => writer.write_all(b"-ERR wrong number of arguments for 'set'\r\n"),
    }
}

fn handle_get<W: Write>(
    writer: &mut W,
    args: &[String],
    store: &Mutex<HashMap<String, (String, Option<Instant>)>>,
) -> io::Result<()> {
    if args.len() != 2 {
        return writer.write_all(b"-ERR wrong number of arguments for 'get'\r\n");
    }

    let key = &args[1];
    let mut map = store.lock().unwrap();
    if let Some((val, opt_expiry)) = map.get(key).cloned() {
        if let Some(expiry) = opt_expiry {
            if Instant::now() >= expiry {
                map.remove(key);
                return writer.write_all(b"$-1\r\n");
            }
        }
        write!(writer, "${}\r\n", val.len())?;
        writer.write_all(val.as_bytes())?;
        writer.write_all(b"\r\n")
    } else {
        writer.write_all(b"$-1\r\n")
    }
}

/// Implements: CONFIG GET <param>
/// Returns: *2\r\n$<len>\r\n<param>\r\n$<len>\r\n<value>\r\n
fn handle_config<W: Write>(
    writer: &mut W,
    args: &[String],
    config: &ServerConfig,
) -> io::Result<()> {
    if args.len() == 3 && args[1].to_uppercase() == "GET" {
        let key = args[2].as_str();
        let value = match key {
            "dir" => &config.dir,
            "dbfilename" => &config.dbfilename,
            _ => {
                return writer.write_all(b"-ERR unknown config parameter\r\n");
            }
        };

        // Array of two Bulk Strings
        write!(writer, "*2\r\n")?;
        write!(writer, "${}\r\n", key.len())?;
        writer.write_all(key.as_bytes())?;
        writer.write_all(b"\r\n")?;
        write!(writer, "${}\r\n", value.len())?;
        writer.write_all(value.as_bytes())?;
        writer.write_all(b"\r\n")
    } else {
        writer.write_all(b"-ERR wrong number of arguments for 'CONFIG'\r\n")
    }
}

fn handle_client(
    stream: TcpStream,
    store: Arc<Mutex<HashMap<String, (String, Option<Instant>)>>>,
    config: Arc<ServerConfig>,
) -> io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = stream;

    while let Some(args) = read_resp_array(&mut reader)? {
        if args.is_empty() {
            continue;
        }

        let cmd = args[0].to_uppercase();
        let res = match cmd.as_str() {
            "PING" => handle_ping(&mut writer),
            "ECHO" => handle_echo(&mut writer, &args),
            "SET" => handle_set(&mut writer, &args, &store),
            "GET" => handle_get(&mut writer, &args, &store),
            "CONFIG" => handle_config(&mut writer, &args, &config),
            _ => writer.write_all(b"-ERR unknown command\r\n"),
        };

        res?;
        writer.flush()?;
    }

    Ok(())
}

fn main() -> io::Result<()> {
    // --- parse our two new flags:
    let mut dir = String::from(".");
    let mut dbfilename = String::from("dump.rdb");
    let args: Vec<String> = env::args().collect();
    let mut i = 1;
    while i + 1 < args.len() {
        match args[i].as_str() {
            "--dir" => dir = args[i + 1].clone(),
            "--dbfilename" => dbfilename = args[i + 1].clone(),
            _ => {}
        }
        i += 2;
    }

    println!("RDB dir = {}, dbfilename = {}", dir, dbfilename);

    let addr = "127.0.0.1:6379";
    let listener = TcpListener::bind(addr)?;
    println!("Listening on {}…", addr);

    let store = Arc::new(Mutex::new(HashMap::new()));
    let config = Arc::new(ServerConfig { dir, dbfilename });

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let store = Arc::clone(&store);
                let config = Arc::clone(&config);
                thread::spawn(move || {
                    if let Err(err) = handle_client(stream, store, config) {
                        eprintln!("Client error: {}", err);
                    }
                });
            }
            Err(err) => eprintln!("Accept error: {}", err),
        }
    }

    Ok(())
}
