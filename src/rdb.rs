use std::{
    collections::HashMap,
    fs::File,
    io::{self, BufRead, BufReader},
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use std::io::Read;

/// Load *all* key/value pairs **and** their optional expiry from the RDB file.
/// Returns `HashMap<key, (value, Option<expiry>)>`.
pub fn load_rdb_snapshot<P: AsRef<Path>>(
    path: P,
) -> io::Result<HashMap<String, (String, Option<SystemTime>)>> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(ref e) if e.kind() == io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(e) => return Err(e),
    };
    let mut rdr = BufReader::new(file);
    let mut map = HashMap::new();

    // 1) Header: must be "REDIS0011"
    let mut hdr = [0u8; 9];
    rdr.read_exact(&mut hdr)?;
    if &hdr != b"REDIS0011" {
        return Ok(map);
    }

    // 2) Skip metadata until DB start (0xFE)
    loop {
        let mut m = [0u8;1];
        if rdr.read_exact(&mut m).is_err() { return Ok(map); }
        match m[0] {
            0xFA => { let _ = read_string(&mut rdr)?; let _ = read_string(&mut rdr)?; },
            0xFE => break,
            _    => {}
        }
    }

    // 3) Drop DB index (size-encoded)
    let _ = read_size(&mut rdr)?;

    // 4) Skip hash-table sizes
    let mut fb = [0u8;1];
    rdr.read_exact(&mut fb)?;
    if fb[0] == 0xFB {
        let _ = read_size(&mut rdr);
        let _ = read_size(&mut rdr);
    }

    // 5) Loop through entries until EOF marker (0xFF)
    loop {
        let buf = rdr.fill_buf()?;
        if buf.is_empty() { break; }
        if buf[0] == 0xFF {
            rdr.consume(1);
            break;
        }

        // a) Optional expire prefix
        let expiry = if buf[0] == 0xFD {
            // 4-byte seconds
            rdr.consume(1);
            let mut x = [0u8;4];
            rdr.read_exact(&mut x)?;
            let secs = u32::from_le_bytes(x) as u64;
            Some(UNIX_EPOCH + Duration::from_secs(secs))
        } else if buf[0] == 0xFC {
            // 8-byte milliseconds
            rdr.consume(1);
            let mut x = [0u8;8];
            rdr.read_exact(&mut x)?;
            let ms = u64::from_le_bytes(x);
            Some(UNIX_EPOCH + Duration::from_millis(ms))
        } else {
            None
        };

        // b) Value‐type byte: only support 0x00 = string
        let mut t = [0u8;1];
        rdr.read_exact(&mut t)?;
        if t[0] != 0x00 {
            // bail on unsupported types
            break;
        }

        // c) Key + Value
        let key = read_string(&mut rdr)?;
        let val = read_string(&mut rdr)?;

        map.insert(key, (val, expiry));
    }

    Ok(map)
}

/// Read a size‐encoded integer (00, 01, 10 prefix).
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
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, ""));
            }
            let len = (((b0 & 0x3F) as usize) << 8) | (buf[1] as usize);
            rdr.consume(2);
            Ok(len)
        }
        2 => {
            if buf.len() < 5 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, ""));
            }
            let mut arr = [0u8;4];
            arr.copy_from_slice(&buf[1..5]);
            rdr.consume(5);
            Ok(u32::from_be_bytes(arr) as usize)
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Unsupported size‐encoding tag",
        )),
    }
}

/// Read either a raw string or an integer‐encoded string (11 prefix).
fn read_string<R: BufRead>(rdr: &mut R) -> io::Result<String> {
    let buf = rdr.fill_buf()?;
    if buf.is_empty() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF on string"));
    }
    let b0 = buf[0];
    let tag = b0 >> 6;

    // Raw string
    if tag < 3 {
        let len = read_size(rdr)?;
        let mut data = vec![0u8; len];
        rdr.read_exact(&mut data)?;
        return String::from_utf8(data)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Bad UTF-8"));
    }

    // Integer encoded
    let enc = b0 & 0x3F;
    rdr.consume(1);
    let s = match enc {
        0 => {
            let mut x = [0u8;1];
            rdr.read_exact(&mut x)?;
            (x[0] as i8).to_string()
        }
        1 => {
            let mut x = [0u8;2];
            rdr.read_exact(&mut x)?;
            i16::from_le_bytes(x).to_string()
        }
        2 => {
            let mut x = [0u8;4];
            rdr.read_exact(&mut x)?;
            i32::from_le_bytes(x).to_string()
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Unsupported integer string encoding",
            ));
        }
    };
    Ok(s)
}
