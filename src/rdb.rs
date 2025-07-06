use std::{
    collections::HashMap,
    fs::File,
    io::{self, BufRead, BufReader},
    path::Path,
};
use std::io::Read;

/// Read a size-encoded integer (6-, 14-, or 32-bit) from the RDB.
fn read_size<R: BufRead>(reader: &mut R) -> io::Result<usize> {
    let mut first = [0u8; 1];
    reader.read_exact(&mut first)?;
    let header = first[0];
    match header >> 6 {
        0 => Ok((header & 0x3F).into()),           // 6-bit
        1 => {                                     // 14-bit
            let mut next = [0u8; 1];
            reader.read_exact(&mut next)?;
            Ok((((header & 0x3F) as usize) << 8) | (next[0] as usize))
        }
        2 => {                                     // 32-bit
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            Ok(u32::from_be_bytes(buf) as usize)
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Unsupported size-encoding tag",
        )),
    }
}

/// Read a string: `<size><bytes…>`
fn read_string<R: BufRead>(reader: &mut R) -> io::Result<String> {
    let len = read_size(reader)?;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    String::from_utf8(buf).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Bad UTF-8"))
}

/// Load a single key/value from the RDB file (if present).
/// Returns an empty map if the file is missing or malformed.
pub(crate) fn load_rdb_snapshot<P: AsRef<Path>>(path: P) -> io::Result<HashMap<String, String>> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(ref e) if e.kind() == io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(e) => return Err(e),
    };
    let mut rdr = BufReader::new(file);

    // 1) Header must be "REDIS0011"
    let mut header = [0u8; 9];
    rdr.read_exact(&mut header)?;
    if &header != b"REDIS0011" {
        return Ok(HashMap::new());
    }

    // 2) Skip metadata subsections until we see 0xFE (DB start)
    loop {
        let mut marker = [0u8; 1];
        if rdr.read_exact(&mut marker).is_err() {
            return Ok(HashMap::new());
        }
        match marker[0] {
            0xFA => { // metadata: skip name+value
                let _ = read_string(&mut rdr)?;
                let _ = read_string(&mut rdr)?;
            }
            0xFE => break, // db start
            _ => {}        // ignore other bytes
        }
    }

    // 3) Read DB index (we ignore the number)
    let _ = read_size(&mut rdr)?;

    // 4) Expect 0xFB then two size-encoded table lengths
    let mut fb = [0u8; 1];
    rdr.read_exact(&mut fb)?;
    if fb[0] == 0xFB {
        let _ = read_size(&mut rdr); // hash table size
        let _ = read_size(&mut rdr); // expires table size
    }

    // 5) Optional expire prefix (0xFD or 0xFC)
    {
        let buf = rdr.fill_buf()?;
        match buf.get(0) {
            Some(&0xFD) => {
                rdr.consume(1);
                let mut tmp = [0u8; 4];
                rdr.read_exact(&mut tmp)?;
            }
            Some(&0xFC) => {
                rdr.consume(1);
                let mut tmp = [0u8; 8];
                rdr.read_exact(&mut tmp)?;
            }
            _ => {}
        }
    }

    // 6) Value-type byte: only support 0x00 = string
    let mut type_byte = [0u8; 1];
    rdr.read_exact(&mut type_byte)?;
    if type_byte[0] != 0x00 {
        return Ok(HashMap::new());
    }

    // 7) Read the key and value strings
    let key = read_string(&mut rdr)?;
    let val = read_string(&mut rdr)?;
    let mut map = HashMap::new();
    map.insert(key, val);
    Ok(map)
}
