use std::{
    collections::HashMap,
    fs::File,
    io::{self, BufRead, BufReader},
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use std::sync::Mutex;

#[derive(Debug, Clone)]
pub enum Value {
    String(String),
    List(Vec<String>),
}

pub(crate) type Store = Mutex<HashMap<String, (Value, Option<SystemTime>)>>;

/// The exact 88-byte “empty” RDB file (hex decoded from the codecrafters asset).
pub(crate) const EMPTY_RDB: &[u8] = b"\x52\x45\x44\x49\x53\x30\x30\x31\x31\xfa\x09\x72\x65\
\x64\x69\x73\x2d\x76\x65\x72\x05\x37\x2e\x32\x2e\x30\xfa\x0a\x72\x65\x64\
\x69\x73\x2d\x62\x69\x74\x73\xc0\x40\xfa\x05\x63\x74\x69\x6d\x65\xc2\x6d\
\x08\xbc\x65\xfa\x08\x75\x73\x65\x64\x2d\x6d\x65\x6d\xc2\xb0\xc4\x10\x00\
\xfa\x08\x61\x6f\x66\x2d\x62\x61\x73\x65\xc0\x00\xff\xf0\x6e\x3b\xfe\xc0\
\xff\x5a\xa2";

/// Load *all* key/value pairs and their optional expiry from the RDB file.
pub fn load_rdb_snapshot<P: AsRef<Path>>(
    path: P,
) -> io::Result<HashMap<String, (Value, Option<SystemTime>)>> {
    let file = match File::open(&path) {
        Ok(f) => f,
        Err(ref e) if e.kind() == io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(e) => return Err(e),
    };
    let mut rdr = BufReader::new(file);

    // 1) Check header
    if !read_header(&mut rdr)? {
        return Ok(HashMap::new());
    }

    // 2) Skip until DB start marker (0xFE)
    skip_metadata(&mut rdr)?;

    // 3) Drop DB index
    drop_db_index(&mut rdr)?;

    // 4) Skip optional hash‐table sizes
    skip_hash_table_sizes(&mut rdr)?;

    // 5) Read all entries until EOF marker
    read_entries(&mut rdr)
}

fn read_header<R: BufRead>(rdr: &mut R) -> io::Result<bool> {
    let mut hdr = [0u8; 9];
    rdr.read_exact(&mut hdr)?;
    // Must match "REDIS0011"
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
                let _ = read_string(rdr)?;
                let _ = read_string(rdr)?;
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
    let mut marker = [0u8; 1];
    rdr.read_exact(&mut marker)?;
    if marker[0] == 0xFB {
        let _ = read_size(rdr)?;
        let _ = read_size(rdr)?;
    }
    Ok(())
}

fn read_entries<R: BufRead>(
    rdr: &mut R,
) -> io::Result<HashMap<String, (Value, Option<SystemTime>)>> {
    let mut map = HashMap::new();

    loop {
        // 1) Grab the first byte without holding the borrow around
        let prefix = {
            let buf = rdr.fill_buf()?;
            if buf.is_empty() {
                break;
            }
            buf[0]
        };

        // 2) EOF marker?
        if prefix == 0xFF {
            rdr.consume(1);
            break;
        }

        // 3) Optional expiry prefix (0xFD or 0xFC)
        let expiry = if prefix == 0xFD || prefix == 0xFC {
            read_expiry_prefix(rdr, prefix)?
        } else {
            None
        };

        // 4) Now read the value‐type byte
        let mut t = [0u8; 1];
        rdr.read_exact(&mut t)?;
        if t[0] != 0x00 {
            break;
        }

        // 5) Key + Value
        let key = read_string(rdr)?;
        let val = read_string(rdr)?;
        map.insert(key, (Value::String(val), expiry));
    }

    Ok(map)
}

fn read_expiry_prefix<R: BufRead>(
    rdr: &mut R,
    prefix_byte: u8,
) -> io::Result<Option<SystemTime>> {
    if prefix_byte == 0xFD {
        rdr.consume(1);
        let mut secs = [0u8; 4];
        rdr.read_exact(&mut secs)?;
        let secs = u32::from_le_bytes(secs) as u64;
        Ok(Some(UNIX_EPOCH + Duration::from_secs(secs)))
    } else if prefix_byte == 0xFC {
        rdr.consume(1);
        let mut ms = [0u8; 8];
        rdr.read_exact(&mut ms)?;
        let ms = u64::from_le_bytes(ms);
        Ok(Some(UNIX_EPOCH + Duration::from_millis(ms)))
    } else {
        Ok(None)
    }
}

/// Read a size-encoded integer (00, 01, 10 prefix).
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
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Unsupported size‐encoding tag",
            ))
        }
    };
    Ok(len)
}

/// Read either a raw string or an integer‐encoded string (11 prefix).
fn read_string<R: BufRead>(rdr: &mut R) -> io::Result<String> {
    let buf = rdr.fill_buf()?;
    if buf.is_empty() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF on string"));
    }
    let b0 = buf[0];
    let tag = b0 >> 6;

    // Raw string (tags 0..2)
    if tag < 3 {
        let len = read_size(rdr)?;
        let mut data = vec![0u8; len];
        rdr.read_exact(&mut data)?;
        return String::from_utf8(data)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Bad UTF-8"));
    }

    // Integer‐encoded (tag == 3)
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
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Unsupported integer string encoding",
            ))
        }
    };
    Ok(s)
}
