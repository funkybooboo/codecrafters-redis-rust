use std::sync::Mutex;
use std::{
    collections::HashMap,
    fs::File,
    io::{self, BufRead, BufReader},
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

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
    let mut marker = [0u8; 1];
    rdr.read_exact(&mut marker)?;
    if marker[0] == 0xFB {
        let ht1 = read_size(rdr)?;
        let ht2 = read_size(rdr)?;
        println!("[rdb::skip_hash_table_sizes] Skipped hash sizes: {}, {}", ht1, ht2);
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
            println!("[rdb::read_entries] Found EOF marker.");
            break;
        }

        let expiry = if prefix == 0xFD || prefix == 0xFC {
            let exp = read_expiry_prefix(rdr, prefix)?;
            println!("[rdb::read_entries] Found expiry: {:?}", exp);
            exp
        } else {
            None
        };

        let mut t = [0u8; 1];
        rdr.read_exact(&mut t)?;
        if t[0] != 0x00 {
            println!("[rdb::read_entries] Unsupported type byte: 0x{:X}", t[0]);
            break;
        }

        let key = read_string(rdr)?;
        let val = read_string(rdr)?;
        println!("[rdb::read_entries] Loaded key: '{}' with value: '{}'", key, val);

        map.insert(key, (Value::String(val), expiry));
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
    let len = match tag {
        0 => {
            rdr.consume(1);
            (b0 & 0x3F) as usize
        }
        1 => {
            if buf.len() < 2 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, ""));
            }
            let val = (((b0 & 0x3F) as usize) << 8) | (buf[1] as usize);
            rdr.consume(2);
            val
        }
        2 => {
            if buf.len() < 5 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, ""));
            }
            let mut arr = [0u8; 4];
            arr.copy_from_slice(&buf[1..5]);
            rdr.consume(5);
            u32::from_be_bytes(arr) as usize
        }
        _ => {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Unsupported size‐encoding tag"));
        }
    };
    println!("[rdb::read_size] Decoded size: {}", len);
    Ok(len)
}

fn read_string<R: BufRead>(rdr: &mut R) -> io::Result<String> {
    let buf = rdr.fill_buf()?;
    if buf.is_empty() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF on string"));
    }

    let b0 = buf[0];
    let tag = b0 >> 6;

    if tag < 3 {
        let len = read_size(rdr)?;
        let mut data = vec![0u8; len];
        rdr.read_exact(&mut data)?;
        let s = String::from_utf8(data)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Bad UTF-8"))?;
        println!("[rdb::read_string] Decoded raw string: '{}'", s);
        return Ok(s);
    }

    rdr.consume(1);
    let s = match b0 & 0x3F {
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
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Unsupported integer string encoding"));
        }
    };
    println!("[rdb::read_string] Decoded integer-encoded string: '{}'", s);
    Ok(s)
}
