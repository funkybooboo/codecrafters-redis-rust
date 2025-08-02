use std::sync::Mutex;
use std::{
    collections::HashMap,
    fs::File,
    io::{self, BufRead, BufReader},
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use std::io::Cursor;

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

/// The exact 88-byte “empty” RDB file (hex decoded from the codecrafters asset).
pub(crate) const EMPTY_RDB: &[u8] = b"\x52\x45\x44\x49\x53\x30\x30\x31\x31\xfa\x09\x72\x65\
\x64\x69\x73\x2d\x76\x65\x72\x05\x37\x2e\x32\x2e\x30\xfa\x0a\x72\x65\x64\
\x69\x73\x2d\x62\x69\x74\x73\xc0\x40\xfa\x05\x63\x74\x69\x6d\x65\xc2\x6d\
\x08\xbc\x65\xfa\x08\x75\x73\x65\x64\x2d\x6d\x65\x6d\xc2\xb0\xc4\x10\x00\
\xfa\x08\x61\x6f\x66\x2d\x62\x61\x73\x65\xc0\x00\xff\xf0\x6e\x3b\xfe\xc0\
\xff\x5a\xa2";

pub fn load_rdb_snapshot<P: AsRef<Path>>(
    path: P,
) -> io::Result<HashMap<String, (Value, Option<SystemTime>)>> {
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

/// Parses an RDB snapshot from in-memory bytes.
/// Returns a HashMap<String, (Value, Option<SystemTime>)> containing the key-value pairs.
pub fn parse_rdb_bytes(bytes: &[u8]) -> io::Result<HashMap<String, (Value, Option<SystemTime>)>> {
    let cursor = Cursor::new(bytes);
    let mut rdr = BufReader::new(cursor);

    if !read_header(&mut rdr)? {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid RDB header"));
    }

    // Skip auxiliary metadata entries
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
                rdr.consume(1); // skip expiry type prefix
                // skip later
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

    let peek = rdr.fill_buf()?;
    if peek.first() == Some(&0xFF) {
        println!("[rdb::parse] No key-value entries. EOF immediately after metadata.");
        rdr.consume(1);
        return Ok(HashMap::new());
    }

    read_entries(&mut rdr)
}

fn read_header<R: BufRead>(rdr: &mut R) -> io::Result<bool> {
    let mut hdr = [0u8; 9];
    rdr.read_exact(&mut hdr)?;
    let valid = &hdr == b"REDIS0011";
    println!("[rdb::read_header] Header check: {}", valid);
    Ok(valid)
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
    println!("[rdb::drop_db_index] DB index skipped.");
    Ok(())
}

fn skip_hash_table_sizes<R: BufRead>(rdr: &mut R) -> io::Result<()> {
    let buf = rdr.fill_buf()?;
    if buf.is_empty() {
        println!("[rdb::skip_hash_table_sizes] EOF reached.");
        return Ok(());
    }

    if buf[0] == 0xFB {
        rdr.consume(1);
        let ht1 = read_size(rdr)?;
        let ht2 = read_size(rdr)?;
        println!("[rdb::skip_hash_table_sizes] Skipped hash sizes: {}, {}", ht1, ht2);
    } else {
        println!("[rdb::skip_hash_table_sizes] Unexpected marker: 0x{:X}", buf[0]);
        rdr.consume(1); // consume unexpected marker
    }

    Ok(())
}

fn read_entries<R: BufRead>(
    rdr: &mut R,
) -> io::Result<HashMap<String, (Value, Option<SystemTime>)>> {
    let mut map = HashMap::new();

    loop {
        let prefix = {
            let buf = rdr.fill_buf()?;
            if buf.is_empty() {
                println!("[rdb::read_entries] EOF reached.");
                break;
            }
            buf[0]
        };

        if prefix == 0xFF {
            rdr.consume(1);
            println!("[rdb::read_entries] Reached end of RDB.");
            break;
        }

        let expiry = match prefix {
            0xFD | 0xFC => {
                read_expiry_prefix(rdr, prefix)?
            }
            _ => None,
        };

        let mut t = [0u8; 1];
        rdr.read_exact(&mut t)?;
        match t[0] {
            0x00 => {
                let key = read_string(rdr)?;
                let val = read_string(rdr)?;
                println!("[rdb::read_entries] Loaded key: '{}' with value: '{}'", key, val);
                map.insert(key, (Value::String(val), expiry));
            }
            0xFF => {
                println!("[rdb::read_entries] Unexpected EOF marker after expiry.");
                break;
            }
            other => {
                return Err(io::Error::new(io::ErrorKind::InvalidData, format!("Unsupported type byte: 0x{:X}", other)));
            }
        }
    }

    Ok(map)
}

fn read_expiry_prefix<R: BufRead>(rdr: &mut R, prefix_byte: u8) -> io::Result<Option<SystemTime>> {
    rdr.consume(1);
    if prefix_byte == 0xFD {
        let mut secs = [0u8; 4];
        rdr.read_exact(&mut secs)?;
        let ts = UNIX_EPOCH + Duration::from_secs(u32::from_le_bytes(secs) as u64);
        Ok(Some(ts))
    } else if prefix_byte == 0xFC {
        let mut ms = [0u8; 8];
        rdr.read_exact(&mut ms)?;
        let ts = UNIX_EPOCH + Duration::from_millis(u64::from_le_bytes(ms));
        Ok(Some(ts))
    } else {
        Ok(None)
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
        3 => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Size-prefixed value cannot use integer-encoded string tag",
        )),
        _ => unreachable!(), // logically unreachable due to 2-bit tag
    }
}

fn read_string<R: BufRead>(rdr: &mut R) -> io::Result<String> {
    let buf = rdr.fill_buf()?;
    if buf.is_empty() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF on string"));
    }

    let b0 = buf[0];
    let tag = b0 >> 6;
    println!(
        "[rdb::read_string] First byte: 0x{:X}, tag: {}, raw: {:?}",
        b0,
        tag,
        &buf[..buf.len().min(8)] // preview up to 8 bytes for context
    );

    if tag == 3 {
        let subtype = b0 & 0x3F;
        println!("[rdb::read_string] Detected integer-encoded string, subtype: 0x{:X}", subtype);
        rdr.consume(1); // consume the type tag byte

        let s = match subtype {
            0 => {
                let mut x = [0u8; 1];
                rdr.read_exact(&mut x)?;
                let val = (x[0] as i8).to_string();
                println!("[rdb::read_string] Decoded 8-bit int: {}", val);
                val
            }
            1 => {
                let mut x = [0u8; 2];
                rdr.read_exact(&mut x)?;
                let val = i16::from_le_bytes(x).to_string();
                println!("[rdb::read_string] Decoded 16-bit int: {}", val);
                val
            }
            2 => {
                let mut x = [0u8; 4];
                rdr.read_exact(&mut x)?;
                let val = i32::from_le_bytes(x).to_string();
                println!("[rdb::read_string] Decoded 32-bit int: {}", val);
                val
            }
            _ => {
                println!(
                    "[rdb::read_string] Unsupported integer-encoded string subtype: 0x{:X}",
                    subtype
                );
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Unsupported integer string encoding: 0x{:X}", subtype),
                ));
            }
        };

        println!("[rdb::read_string] Decoded integer-encoded string: '{}'", s);
        Ok(s)
    } else {
        println!("[rdb::read_string] Detected raw string, parsing size...");
        let len = read_size(rdr)?;
        println!("[rdb::read_string] Raw string length: {}", len);
        let mut data = vec![0u8; len];
        rdr.read_exact(&mut data)?;
        let s = String::from_utf8(data)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Bad UTF-8"))?;
        println!("[rdb::read_string] Decoded raw string: '{}'", s);
        Ok(s)
    }
}
