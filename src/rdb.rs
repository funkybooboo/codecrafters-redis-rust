use std::{
    collections::HashMap,
    fs::File,
    io::{self, BufRead, BufReader},
    path::Path,
};
use std::io::Read;

/// Load *all* key/value pairs from the RDB snapshot, or return an empty map.
/// Stops when it encounters the EOF marker (0xFF).
pub fn load_rdb_snapshot<P: AsRef<Path>>(path: P) -> io::Result<HashMap<String, String>> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(ref e) if e.kind() == io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(e) => return Err(e),
    };
    let mut rdr = BufReader::new(file);
    let mut result = HashMap::new();

    // 1) Header: must be "REDIS0011"
    let mut header = [0u8; 9];
    rdr.read_exact(&mut header)?;
    if &header != b"REDIS0011" {
        return Ok(result);
    }

    // 2) Skip metadata until DB start (0xFE)
    loop {
        let mut marker = [0u8; 1];
        if rdr.read_exact(&mut marker).is_err() {
            return Ok(result);
        }
        match marker[0] {
            0xFA => { let _ = read_string(&mut rdr)?; let _ = read_string(&mut rdr)?; },
            0xFE => break,
            _    => {}
        }
    }

    // 3) Read DB index (size-encoded) and drop it
    let _ = read_size(&mut rdr)?;

    // 4) Expect 0xFB then two size-encoded lengths
    let mut fb = [0u8; 1];
    rdr.read_exact(&mut fb)?;
    if fb[0] == 0xFB {
        let _ = read_size(&mut rdr); // hash table size
        let _ = read_size(&mut rdr); // expires table size
    }

    // 5) Now loop: read entries until we hit 0xFF (EOF marker)
    loop {
        // peek next byte
        let buf = rdr.fill_buf()?;
        if buf.is_empty() {
            break;
        }
        match buf[0] {
            0xFF => {
                // End-of-file marker; consume it and break
                rdr.consume(1);
                break;
            }
            0xFD => {
                // 4-byte expire (seconds)
                rdr.consume(1);
                let mut tmp = [0u8; 4];
                rdr.read_exact(&mut tmp)?;
            }
            0xFC => {
                // 8-byte expire (milliseconds)
                rdr.consume(1);
                let mut tmp = [0u8; 8];
                rdr.read_exact(&mut tmp)?;
            }
            _ => {}
        }

        // 6) Value-type byte: only support strings (0x00)
        let mut typ = [0u8; 1];
        rdr.read_exact(&mut typ)?;
        if typ[0] != 0x00 {
            // skip unsupported types by bailing out
            break;
        }

        // 7) Read the key and value strings
        let key = read_string(&mut rdr)?;
        let val = read_string(&mut rdr)?;
        result.insert(key, val);
    }

    Ok(result)
}

/// Read a size-encoded integer (00, 01, or 10 prefix).
fn read_size<R: BufRead>(rdr: &mut R) -> io::Result<usize> {
    let buf = rdr.fill_buf()?;
    if buf.is_empty() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF on size"));
    }
    let b0 = buf[0];
    let tag = b0 >> 6;
    match tag {
        0 => {
            let len = (b0 & 0x3F) as usize;
            rdr.consume(1);
            Ok(len)
        }
        1 => {
            if buf.len() < 2 { return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "")); }
            let hi = (b0 & 0x3F) as usize;
            let lo = buf[1] as usize;
            rdr.consume(2);
            Ok((hi << 8) | lo)
        }
        2 => {
            if buf.len() < 5 { return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "")); }
            let mut arr = [0u8; 4];
            arr.copy_from_slice(&buf[1..5]);
            rdr.consume(5);
            Ok(u32::from_be_bytes(arr) as usize)
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Unsupported size-encoding tag",
        )),
    }
}

/// Read a Redis-encoded string: either raw (00/01/10) or integer-encoded (11).
fn read_string<R: BufRead>(rdr: &mut R) -> io::Result<String> {
    let buf = rdr.fill_buf()?;
    if buf.is_empty() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF on string"));
    }
    let b0 = buf[0];
    let tag = b0 >> 6;

    // RAW STRING
    if tag < 3 {
        let len = read_size(rdr)?;
        let mut data = vec![0u8; len];
        rdr.read_exact(&mut data)?;
        return String::from_utf8(data)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Bad UTF-8"));
    }

    // INTEGER-ENCODED STRING (11)
    let encoding = b0 & 0x3F;
    rdr.consume(1);
    let s = match encoding {
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
