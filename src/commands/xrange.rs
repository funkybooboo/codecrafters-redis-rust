use std::io;
use std::io::Write;
use std::net::TcpStream;
use crate::commands::Context;
use crate::rdb::{StreamEntry, Value};
use crate::resp::{write_bulk_string, write_error};

pub fn cmd_xrange(
    out: &mut TcpStream,
    args: &[String],
    ctx: &Context,
) -> io::Result<()> {
    // 1) Usage check
    if args.len() != 4 {
        write_error(out, "usage: XRANGE <key> <start> <end>")?;
        return Ok(());
    }
    let key       = &args[1];
    let start_raw = &args[2];
    let end_raw   = &args[3];

    // 2) Clone entries or early‐return
    let entries: Vec<StreamEntry> = {
        let map = ctx.store.lock().unwrap();
        match map.get(key) {
            None => { out.write_all(b"*0\r\n")?; return Ok(()); }
            Some((Value::Stream(v), _)) => v.clone(),
            Some(_) => { write_error(out, "WRONGTYPE Operation against a key holding the wrong kind of value")?; return Ok(()); }
        }
    };

    // 3) Parse bounds
    fn parse_start(raw: &str) -> Result<(u64, u64), ()> {
        if raw == "-" {
            Ok((0, 0))
        } else if let Some(_) = raw.find('-') {
            let mut p = raw.splitn(2, '-');
            let ms  = p.next().unwrap().parse().map_err(|_| ())?;
            let seq = p.next().unwrap().parse().map_err(|_| ())?;
            Ok((ms, seq))
        } else {
            let ms = raw.parse().map_err(|_| ())?;
            Ok((ms, 0))
        }
    }

    fn parse_end(raw: &str, entries: &[StreamEntry]) -> Result<(u64, u64), ()> {
        if raw == "+" {
            // to the end of the stream
            Ok((u64::MAX, u64::MAX))
        } else if let Some(_) = raw.find('-') {
            let mut p = raw.splitn(2, '-');
            let ms  = p.next().unwrap().parse().map_err(|_| ())?;
            let seq = p.next().unwrap().parse().map_err(|_| ())?;
            Ok((ms, seq))
        } else {
            let ms = raw.parse().map_err(|_| ())?;
            // find max sequence at this ms
            let max_seq = entries.iter()
                .filter_map(|e| {
                    let mut p = e.id.splitn(2, '-');
                    let t = p.next().and_then(|s| s.parse::<u64>().ok())?;
                    let s = p.next().and_then(|s| s.parse::<u64>().ok())?;
                    if t == ms { Some(s) } else { None }
                })
                .max()
                .unwrap_or(0);
            Ok((ms, max_seq))
        }
    }

    let (start_ms, start_seq) = match parse_start(start_raw) {
        Ok(x) => x,
        Err(_) => { write_error(out, "ERR invalid start ID format")?; return Ok(()); }
    };
    let (end_ms, end_seq) = match parse_end(end_raw, &entries) {
        Ok(x) => x,
        Err(_) => { write_error(out, "ERR invalid end ID format")?; return Ok(()); }
    };

    // 4) Filter inclusive
    let filtered: Vec<StreamEntry> = entries.into_iter()
        .filter(|e| {
            let mut p   = e.id.splitn(2, '-');
            let ems  = p.next().and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
            let eseq = p.next().and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
            (ems > start_ms  || (ems == start_ms  && eseq >= start_seq)) &&
                (ems < end_ms    || (ems == end_ms    && eseq <= end_seq))
        })
        .collect();

    // 5) Write RESP
    write!(out, "*{}\r\n", filtered.len())?;
    for entry in filtered {
        // [ ID , [k,v,k,v,…] ]
        write!(out, "*2\r\n")?;
        write_bulk_string(out, &entry.id)?;
        let kvs = entry.fields.len() * 2;
        write!(out, "*{}\r\n", kvs)?;
        for (k, v) in entry.fields {
            write_bulk_string(out, &k)?;
            write_bulk_string(out, &v)?;
        }
    }

    Ok(())
}
