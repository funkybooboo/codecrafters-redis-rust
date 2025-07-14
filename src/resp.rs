use std::io::{self, BufRead, Write};

/// Read one RESP Array of Bulk Strings.
/// Returns `Ok(Some(vec![]))` on an empty/malformed array header,
/// `Ok(None)` on EOF, or `Err` on other I/O errors.
pub(crate) fn read_array<R: BufRead>(reader: &mut R) -> io::Result<Option<Vec<String>>> {
    // Read the `*<count>\r\n` line
    let mut header = String::new();
    if reader.read_line(&mut header)? == 0 {
        return Ok(None); // EOF
    }
    let header = header.trim_end(); // remove \r\n

    // If it doesn’t start with '*', treat as empty args
    if !header.starts_with('*') {
        return Ok(Some(Vec::new()));
    }
    let count: usize = header[1..]
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid array count"))?;

    // Read that many Bulk Strings
    let mut args = Vec::with_capacity(count);
    for _ in 0..count {
        // Bulk header: `$<len>\r\n`
        let mut bulk_header = String::new();
        reader.read_line(&mut bulk_header)?;
        let bulk_header = bulk_header.trim_end();
        if !bulk_header.starts_with('$') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Expected bulk string header",
            ));
        }
        let len: usize = bulk_header[1..]
            .parse()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid bulk length"))?;

        // Read `<data>\r\n`
        let mut buf = vec![0; len + 2];
        reader.read_exact(&mut buf)?;
        let text = String::from_utf8(buf[..len].to_vec())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid UTF-8"))?;
        args.push(text);
    }

    Ok(Some(args))
}

/// *<N>\r\n then N bulk‐strings
pub fn write_array(out: &mut dyn Write, items: &[&str]) -> io::Result<()> {
    write!(out, "*{}\r\n", items.len())?;
    for &item in items {
        write_bulk_string(out, item)?;
    }
    Ok(())
}

/// +<string>\r\n
pub fn write_simple_string(out: &mut dyn Write, s: &str) -> io::Result<()> {
    write!(out, "+{}\r\n", s)
}

/// -ERR <msg>\r\n
pub fn write_error(out: &mut dyn Write, msg: &str) -> io::Result<()> {
    write!(out, "-ERR {}\r\n", msg)
}

/// $<len>\r\n<data>\r\n
pub fn write_bulk_string(out: &mut dyn Write, data: &str) -> io::Result<()> {
    write!(out, "${}\r\n{}\r\n", data.len(), data)
}

/// Return `false` (and write an ERR) if `args.len() != expected`.
pub fn check_len(
    out: &mut dyn Write,
    args: &[String],
    expected: usize,
    usage: &str,
) -> bool {
    if args.len() != expected {
        let _ = write_error(out, usage);
        false
    } else {
        true
    }
}
