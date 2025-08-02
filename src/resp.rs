use std::io::{self, BufRead, Write};
use memchr::memchr;

/// Read one RESP Array of Bulk Strings.
/// Returns `Ok(Some(vec![]))` on an empty/malformed array header,
/// `Ok(None)` on EOF, or `Err` on other I/O errors.
pub(crate) fn read_resp_array<R: BufRead>(reader: &mut R) -> io::Result<Option<Vec<String>>> {
    let mut header = String::new();
    if reader.read_line(&mut header)? == 0 {
        println!("[resp::read_resp_array] EOF reached");
        return Ok(None);
    }
    let header = header.trim_end();
    println!("[resp::read_resp_array] Header: '{}'", header);

    if !header.starts_with('*') {
        println!("[resp::read_resp_array] Not an array, returning empty vec");
        return Ok(Some(Vec::new()));
    }

    let count: usize = header[1..]
        .parse()
        .map_err(|_| {
            eprintln!("[resp::read_resp_array] Invalid array count: '{}'", &header[1..]);
            io::Error::new(io::ErrorKind::InvalidData, "Invalid array count")
        })?;
    println!("[resp::read_resp_array] Parsing {} bulk string(s)", count);

    let mut args = Vec::with_capacity(count);
    for i in 0..count {
        let mut bulk_header = String::new();
        reader.read_line(&mut bulk_header)?;
        let bulk_header = bulk_header.trim_end();
        println!("[resp::read_resp_array] Bulk header {}: '{}'", i, bulk_header);

        if !bulk_header.starts_with('$') {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Expected bulk string header"));
        }

        let len: usize = bulk_header[1..].parse().map_err(|_| {
            eprintln!("[resp::read_resp_array] Invalid bulk length: '{}'", &bulk_header[1..]);
            io::Error::new(io::ErrorKind::InvalidData, "Invalid bulk length")
        })?;

        let mut buf = vec![0; len + 2];
        reader.read_exact(&mut buf)?;
        let text = String::from_utf8(buf[..len].to_vec()).map_err(|_| {
            eprintln!("[resp::read_resp_array] Invalid UTF-8 in bulk string");
            io::Error::new(io::ErrorKind::InvalidData, "Invalid UTF-8")
        })?;
        println!("[resp::read_resp_array] Parsed argument {}: '{}'", i, text);
        args.push(text);
    }

    Ok(Some(args))
}

/// *<N>\r\n then N bulk‐strings
pub fn write_resp_array(out: &mut dyn Write, items: &[&str]) -> io::Result<()> {
    write!(out, "*{}\r\n", items.len())?;
    for &item in items {
        write_bulk_resp_string(out, item)?;
    }
    Ok(())
}

/// +<string>\r\n
pub fn write_simple_resp_string(out: &mut dyn Write, s: &str) -> io::Result<()> {
    write!(out, "+{s}\r\n")
}

/// -ERR <msg>\r\n
pub fn write_resp_error(out: &mut dyn Write, msg: &str) -> io::Result<()> {
    write!(out, "-ERR {msg}\r\n")
}

/// $<len>\r\n<data>\r\n
pub fn write_bulk_resp_string(out: &mut dyn Write, data: &str) -> io::Result<()> {
    write!(out, "${}\r\n{}\r\n", data.len(), data)
}

/// Reads and returns the exact size (in bytes) of the next RESP command in the reader's buffer.
/// Returns `Ok(0)` if the full command isn't buffered yet.
/// Returns `Err(UnexpectedEof)` if the stream is closed (EOF).
pub fn peek_resp_command_size<R: BufRead>(reader: &mut R) -> io::Result<usize> {
    let buf = reader.fill_buf()?;
    if buf.is_empty() {
        println!("[resp::peek_resp_command_size] EOF reached");
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF reached"));
    }

    let array_end = match memchr(b'\n', buf) {
        Some(pos) => pos + 1,
        None => return Ok(0), // not enough data for array header
    };

    let header_str = std::str::from_utf8(&buf[..array_end - 2]).map_err(|_| {
        eprintln!("[resp::peek_resp_command_size] Invalid UTF-8 in array header");
        io::Error::new(io::ErrorKind::InvalidData, "Invalid UTF-8")
    })?;

    if !header_str.starts_with('*') {
        eprintln!("[resp::peek_resp_command_size] Not a RESP array: '{}'", header_str);
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Expected array header"));
    }

    let num_elements: usize = header_str[1..].parse().map_err(|_| {
        eprintln!("[resp::peek_resp_command_size] Invalid array count: '{}'", &header_str[1..]);
        io::Error::new(io::ErrorKind::InvalidData, "Invalid array count")
    })?;
    println!("[resp::peek_resp_command_size] Command has {} element(s)", num_elements);

    let mut current_pos = array_end;

    for i in 0..num_elements {
        let remaining = &buf[current_pos..];
        let bulk_end = match memchr(b'\n', remaining) {
            Some(pos) => pos + 1,
            None => return Ok(0), // wait for full bulk header
        };

        let bulk_header_str = std::str::from_utf8(&remaining[..bulk_end - 2]).map_err(|_| {
            eprintln!("[resp::peek_resp_command_size] Invalid UTF-8 in bulk header");
            io::Error::new(io::ErrorKind::InvalidData, "Invalid UTF-8")
        })?;

        if !bulk_header_str.starts_with('$') {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Expected bulk string header"));
        }

        let bulk_len: usize = bulk_header_str[1..].parse().map_err(|_| {
            eprintln!("[resp::peek_resp_command_size] Invalid bulk length: '{}'", &bulk_header_str[1..]);
            io::Error::new(io::ErrorKind::InvalidData, "Invalid bulk length")
        })?;

        current_pos += bulk_end;
        current_pos += bulk_len + 2;

        if buf.len() < current_pos {
            println!(
                "[resp::peek_resp_command_size] Incomplete command: need {}, got {} — waiting for more data",
                current_pos,
                buf.len()
            );
            return Ok(0);
        }

        println!("[resp::peek_resp_command_size] Bulk string {} size: {}", i, bulk_len);
    }

    println!("[resp::peek_resp_command_size] Total command size: {}", current_pos);
    Ok(current_pos)
}

pub fn encode_bulk_resp_string(s: &str) -> Vec<u8> {
    format!("${}\r\n{}\r\n", s.len(), s).into_bytes()
}

pub fn encode_resp_array(chunks: &[Vec<u8>]) -> Vec<u8> {
    let mut result = format!("*{}\r\n", chunks.len()).into_bytes();
    for c in chunks {
        result.extend_from_slice(c);
    }
    result
}

pub fn encode_resp_error(msg: &str) -> Vec<u8> {
    format!("-ERR {}\r\n", msg).into_bytes()
}

/// +<string>\r\n
pub fn encode_simple_resp_string(s: &str) -> Vec<u8> {
    format!("+{}\r\n", s).into_bytes()
}

/// Encodes an integer in RESP format: `:<number>\r\n`
pub fn encode_int(n: i64) -> Vec<u8> {
    format!(":{}\r\n", n).into_bytes()
}
