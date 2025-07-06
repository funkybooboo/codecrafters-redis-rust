use std::io::{self, BufRead, Write};

/// Read one RESP Array of Bulk Strings.
/// Returns `Ok(Some(vec![]))` on an empty/malformed array header,
/// `Ok(None)` on EOF, or `Err` on other I/O errors.
pub(crate) fn read_resp_array<R: BufRead>(reader: &mut R) -> io::Result<Option<Vec<String>>> {
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

/// Send a Bulk String response: `$<len>\r\n<data>\r\n`
pub(crate) fn write_bulk_string<W: Write>(writer: &mut W, data: &str) -> io::Result<()> {
    write!(writer, "${}\r\n", data.len())?;
    writer.write_all(data.as_bytes())?;
    writer.write_all(b"\r\n")
}
