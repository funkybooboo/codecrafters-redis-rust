use crate::commands::Context;
use crate::rdb::{StreamEntry, Value};
use crate::resp::{write_bulk_string, write_error};
use std::io::Write;
use std::net::TcpStream;
use std::time::{Duration, Instant};
use std::{io, thread};

/// XREAD [BLOCK <ms>] STREAMS <key> [<key> ...] <id> [<id> ...]
pub fn cmd_xread(out: &mut TcpStream, args: &[String], ctx: &Context) -> io::Result<()> {
    // 1) Parse optional BLOCK
    let mut idx = 1;
    let block_ms = if args.get(idx).map(|s| s.to_lowercase()) == Some("block".into()) {
        let ms = args
            .get(idx + 1)
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        idx += 2;
        Some(ms)
    } else {
        None
    };

    // 2) Expect STREAMS
    if args.get(idx).map(|s| s.to_lowercase()) != Some("streams".into()) {
        write_error(
            out,
            "usage: XREAD [BLOCK <ms>] STREAMS <key> [<key> ...] <id> [<id> ...]",
        )?;
        return Ok(());
    }
    idx += 1;

    // 3) Split keys vs starts
    let rem = args.len() - idx;
    if rem < 2 || rem % 2 != 0 {
        write_error(
            out,
            "usage: XREAD [BLOCK <ms>] STREAMS <key> [<key> ...] <id> [<id> ...]",
        )?;
        return Ok(());
    }
    let n_streams = rem / 2;
    let keys = &args[idx..idx + n_streams];
    let starts = &args[idx + n_streams..];

    // 4) **Compute static start Positions** (ms,seq) for each stream **once**
    let mut start_positions = Vec::with_capacity(n_streams);
    {
        let store = ctx.store.lock().unwrap();
        for (key, start_raw) in keys.iter().zip(starts.iter()) {
            // Fetch existing entries (type-error if not a stream)
            let entries = match store.get(key) {
                Some((Value::Stream(v), _)) => v.clone(),
                Some(_) => {
                    write_error(
                        out,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )?;
                    return Ok(());
                }
                None => Vec::new(),
            };

            // Determine (start_ms, start_seq)
            let start_pos = if start_raw == "$" {
                // Last ID at call time
                entries.iter().fold((0u64, 0u64), |(lm, ls), e| {
                    let mut p = e.id.splitn(2, '-');
                    if let (Some(ms_s), Some(seq_s)) = (p.next(), p.next()) {
                        if let (Ok(ms), Ok(seq)) = (ms_s.parse(), seq_s.parse()) {
                            if ms > lm || (ms == lm && seq > ls) {
                                return (ms, seq);
                            }
                        }
                    }
                    (lm, ls)
                })
            } else if let Some(_) = start_raw.find('-') {
                // explicit "ms-seq"
                let mut p = start_raw.splitn(2, '-');
                let ms = p.next().unwrap().parse().unwrap_or(0);
                let seq = p.next().unwrap().parse().unwrap_or(0);
                (ms, seq)
            } else {
                // ms-only => seq=0
                let ms = start_raw.parse().unwrap_or(0);
                (ms, 0)
            };

            start_positions.push(start_pos);
        }
    }
    // store lock dropped here

    // 5) Closure to collect all new entries > static start positions
    let collect = || -> Result<Vec<(String, Vec<StreamEntry>)>, ()> {
        let store = ctx.store.lock().unwrap();
        let mut out = Vec::with_capacity(n_streams);

        for (key, &(start_ms, start_seq)) in keys.iter().zip(start_positions.iter()) {
            // Fetch entries
            let entries = match store.get(key) {
                Some((Value::Stream(v), _)) => v.clone(),
                Some(_) => return Err(()),
                None => Vec::new(),
            };

            // Filter IDs strictly greater than (start_ms, start_seq)
            let filtered = entries
                .into_iter()
                .filter(|e| {
                    let mut p = e.id.splitn(2, '-');
                    let ems = p.next().and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
                    let eseq = p.next().and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
                    ems > start_ms || (ems == start_ms && eseq > start_seq)
                })
                .collect::<Vec<_>>();

            if !filtered.is_empty() {
                out.push((key.clone(), filtered));
            }
        }
        Ok(out)
    };

    // 6) Run once immediately
    let mut results = match collect() {
        Ok(r) => r,
        Err(_) => {
            write_error(
                out,
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )?;
            return Ok(());
        }
    };

    // 7) If empty & blocking requested
    if results.is_empty() {
        match block_ms {
            None => {
                // no BLOCK → empty array
                out.write_all(b"*0\r\n")?;
                return Ok(());
            }
            Some(0) => {
                // BLOCK 0 => wait forever
                loop {
                    thread::sleep(Duration::from_millis(10));
                    results = match collect() {
                        Ok(r) => r,
                        Err(_) => {
                            write_error(
                                out,
                                "WRONGTYPE Operation against a key holding the wrong kind of value",
                            )?;
                            return Ok(());
                        }
                    };
                    if !results.is_empty() {
                        break;
                    }
                }
            }
            Some(ms) => {
                // BLOCK ms>0 => timed wait
                let deadline = Instant::now() + Duration::from_millis(ms);
                while Instant::now() < deadline {
                    thread::sleep(Duration::from_millis(10));
                    results = match collect() {
                        Ok(r) => r,
                        Err(_) => {
                            write_error(
                                out,
                                "WRONGTYPE Operation against a key holding the wrong kind of value",
                            )?;
                            return Ok(());
                        }
                    };
                    if !results.is_empty() {
                        break;
                    }
                }
                if results.is_empty() {
                    // timed out → null bulk
                    out.write_all(b"$-1\r\n")?;
                    return Ok(());
                }
            }
        }
    }

    // 8) Encode final results
    write!(out, "*{}\r\n", results.len())?;
    for (key, entries) in results {
        write!(out, "*2\r\n")?;
        write_bulk_string(out, &key)?;
        write!(out, "*{}\r\n", entries.len())?;
        for entry in entries {
            write!(out, "*2\r\n")?;
            write_bulk_string(out, &entry.id)?;
            let kvs = entry.fields.len() * 2;
            write!(out, "*{}\r\n", kvs)?;
            for (k, v) in entry.fields {
                write_bulk_string(out, &k)?;
                write_bulk_string(out, &v)?;
            }
        }
    }

    Ok(())
}
