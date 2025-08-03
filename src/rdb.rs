use crate::context::Context;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Cursor, Read};
use std::net::TcpStream;
use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct StreamEntry {
    pub id: String,
    pub fields: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub enum Value {
    String(String),
    List(Vec<String>),
    Stream(Vec<StreamEntry>),
}

pub(crate) type Store = Mutex<HashMap<String, (Value, Option<SystemTime>)>>;

pub(crate) const EMPTY_RDB: &[u8] = b"\x52\x45\x44\x49\x53\x30\x30\x31\x31\xfa\x09\x72\x65\
\x64\x69\x73\x2d\x76\x65\x72\x05\x37\x2e\x32\x2e\x30\xfa\x0a\x72\x65\x64\
\x69\x73\x2d\x62\x69\x74\x73\xc0\x40\xfa\x05\x63\x74\x69\x6d\x65\xc2\x6d\
\x08\xbc\x65\xfa\x08\x75\x73\x65\x64\x2d\x6d\x65\x6d\xc2\xb0\xc4\x10\x00\
\xfa\x08\x61\x6f\x66\x2d\x62\x61\x73\x65\xc0\x00\xff\xf0\x6e\x3b\xfe\xc0\
\xff\x5a\xa2";

pub fn load_rdb_snapshot_from_stream(reader: &mut BufReader<TcpStream>, ctx: &mut Context) -> io::Result<()> {
    let mut rdb_header = String::new();
    reader.read_line(&mut rdb_header)?;
    println!("[replication::rdb] RDB header: {}", rdb_header.trim());

    if !rdb_header.starts_with('$') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Expected '$' prefix for RDB header, got: '{}'", rdb_header.trim()),
        ));
    }

    let rdb_len: usize = rdb_header[1..].trim().parse().map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidData, "Invalid RDB length in header")
    })?;

    let mut rdb_buf = vec![0; rdb_len];
    reader.read_exact(&mut rdb_buf)?;
    println!("[replication::rdb] Snapshot read ({} bytes).", rdb_len);

    let parsed = parse_rdb_bytes(&rdb_buf)?;
    *ctx.store.lock().unwrap() = parsed;
    println!("[replication::rdb] Snapshot loaded into store successfully.");

    Ok(())
}

pub fn load_rdb_snapshot_from_path<P: AsRef<Path>>(path: P) -> io::Result<HashMap<String, (Value, Option<SystemTime>)>> {
    println!("[rdb::load_rdb_snapshot] Loading snapshot from {:?}", path.as_ref());

    let file = match File::open(&path) {
        Ok(f) => {
            println!("[rdb::load_rdb_snapshot] File opened successfully.");
            f
        }
        Err(ref e) if e.kind() == io::ErrorKind::NotFound => {
            println!("[rdb::load_rdb_snapshot] Snapshot file not found. Returning empty store.");
            return Ok(HashMap::new());
        }
        Err(e) => return Err(e),
    };

    let mut rdr = BufReader::new(file);

    if !read_header(&mut rdr)? {
        println!("[rdb::load_rdb_snapshot] Invalid or missing RDB header.");
        return Ok(HashMap::new());
    }

    println!("[rdb::load_rdb_snapshot] Header valid. Continuing…");

    skip_metadata(&mut rdr)?;
    println!("[rdb::load_rdb_snapshot] Metadata skipped.");

    drop_db_index(&mut rdr)?;
    println!("[rdb::load_rdb_snapshot] DB index dropped.");

    skip_hash_table_sizes(&mut rdr)?;
    println!("[rdb::load_rdb_snapshot] Hash table sizes skipped.");

    let entries = read_entries(&mut rdr)?;
    println!("[rdb::load_rdb_snapshot] Finished reading {} entries.", entries.len());

    Ok(entries)
}

fn parse_rdb_bytes(bytes: &[u8]) -> io::Result<HashMap<String, (Value, Option<SystemTime>)>> {
    let cursor = Cursor::new(bytes);
    let mut rdr = BufReader::new(cursor);

    if !read_header(&mut rdr)? {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid RDB header"));
    }

    loop {
        let byte = rdr.fill_buf()?;
        if byte.is_empty() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Unexpected EOF while parsing metadata"));
        }

        match byte[0] {
            0xFA => {
                rdr.consume(1);
                let key = read_string(&mut rdr)?;
                let val = read_string(&mut rdr)?;
                println!("[rdb::parse] Skipped metadata: {} = {}", key, val);
            }
            0xFE => {
                rdr.consume(1);
                drop_db_index(&mut rdr)?;
            }
            0xFB | 0xFC => {
                rdr.consume(1);
            }
            0xFF => {
                println!("[rdb::parse] Reached EOF marker during metadata.");
                rdr.consume(1);
                return Ok(HashMap::new());
            }
            _ => break,
        }
    }

    skip_hash_table_sizes(&mut rdr)?;

    if rdr.fill_buf()?.first() == Some(&0xFF) {
        println!("[rdb::parse] No key-value entries. EOF immediately after metadata.");
        rdr.consume(1);
        return Ok(HashMap::new());
    }

    read_entries(&mut rdr)
}

fn read_header<R: BufRead>(rdr: &mut R) -> io::Result<bool> {
    let mut hdr = [0u8; 9];
    rdr.read_exact(&mut hdr)?;
    Ok(&hdr == b"REDIS0011")
}

fn skip_metadata<R: BufRead>(rdr: &mut R) -> io::Result<()> {
    let mut byte = [0u8; 1];
    loop {
        if rdr.read_exact(&mut byte).is_err() {
            return Ok(()); // EOF
        }
        match byte[0] {
            0xFA => {
                let key = read_string(rdr)?;
                let val = read_string(rdr)?;
                println!("[rdb::skip_metadata] Skipped metadata: {} = {}", key, val);
            }
            0xFE => break,
            _ => {}
        }
    }
    Ok(())
}

fn drop_db_index<R: BufRead>(rdr: &mut R) -> io::Result<()> {
    let _ = read_size(rdr)?;
    Ok(())
}

fn skip_hash_table_sizes<R: BufRead>(rdr: &mut R) -> io::Result<()> {
    let buf = rdr.fill_buf()?;
    if buf.is_empty() {
        return Ok(());
    }

    if buf[0] == 0xFB {
        rdr.consume(1);
        let _ = read_size(rdr)?;
        let _ = read_size(rdr)?;
    } else {
        rdr.consume(1); // consume unknown marker
    }

    Ok(())
}

fn read_entries<R: BufRead>(rdr: &mut R) -> io::Result<HashMap<String, (Value, Option<SystemTime>)>> {
    let mut map = HashMap::new();

    loop {
        let prefix = {
            let buf = rdr.fill_buf()?;
            if buf.is_empty() {
                break;
            }
            buf[0]
        };

        if prefix == 0xFF {
            rdr.consume(1);
            break;
        }

        let expiry = match prefix {
            0xFD | 0xFC => read_expiry_prefix(rdr, prefix)?,
            _ => None,
        };

        let mut t = [0u8; 1];
        rdr.read_exact(&mut t)?;
        match t[0] {
            0x00 => {
                let key = read_string(rdr)?;
                let val = read_string(rdr)?;
                map.insert(key, (Value::String(val), expiry));
            }
            0xFF => break,
            other => {
                return Err(io::Error::new(io::ErrorKind::InvalidData, format!("Unsupported type byte: 0x{:X}", other)));
            }
        }
    }

    Ok(map)
}

fn read_expiry_prefix<R: BufRead>(rdr: &mut R, prefix: u8) -> io::Result<Option<SystemTime>> {
    rdr.consume(1);
    match prefix {
        0xFD => {
            let mut secs = [0u8; 4];
            rdr.read_exact(&mut secs)?;
            Ok(Some(UNIX_EPOCH + Duration::from_secs(u32::from_le_bytes(secs) as u64)))
        }
        0xFC => {
            let mut ms = [0u8; 8];
            rdr.read_exact(&mut ms)?;
            Ok(Some(UNIX_EPOCH + Duration::from_millis(u64::from_le_bytes(ms))))
        }
        _ => Ok(None),
    }
}

fn read_size<R: BufRead>(rdr: &mut R) -> io::Result<usize> {
    let buf = rdr.fill_buf()?;
    if buf.is_empty() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF on size"));
    }

    let b0 = buf[0];
    let tag = b0 >> 6;

    match tag {
        0 => {
            rdr.consume(1);
            Ok((b0 & 0x3F) as usize)
        }
        1 => {
            if buf.len() < 2 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Need 2 bytes for 14-bit length"));
            }
            let val = (((b0 & 0x3F) as usize) << 8) | (buf[1] as usize);
            rdr.consume(2);
            Ok(val)
        }
        2 => {
            if buf.len() < 5 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Need 5 bytes for 32-bit length"));
            }
            let mut arr = [0u8; 4];
            arr.copy_from_slice(&buf[1..5]);
            rdr.consume(5);
            Ok(u32::from_be_bytes(arr) as usize)
        }
        _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid size tag")),
    }
}

fn read_string<R: BufRead>(rdr: &mut R) -> io::Result<String> {
    let buf = rdr.fill_buf()?;
    if buf.is_empty() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF on string"));
    }

    let b0 = buf[0];
    let tag = b0 >> 6;

    if tag == 3 {
        rdr.consume(1);
        let subtype = b0 & 0x3F;

        let val = match subtype {
            0 => {
                let mut x = [0u8; 1];
                rdr.read_exact(&mut x)?;
                (x[0] as i8).to_string()
            }
            1 => {
                let mut x = [0u8; 2];
                rdr.read_exact(&mut x)?;
                i16::from_le_bytes(x).to_string()
            }
            2 => {
                let mut x = [0u8; 4];
                rdr.read_exact(&mut x)?;
                i32::from_le_bytes(x).to_string()
            }
            _ => {
                return Err(io::Error::new(io::ErrorKind::InvalidData, format!("Unsupported integer string encoding: 0x{:X}", subtype)));
            }
        };

        Ok(val)
    } else {
        let len = read_size(rdr)?;
        let mut data = vec![0u8; len];
        rdr.read_exact(&mut data)?;
        String::from_utf8(data).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Bad UTF-8"))
    }
}
