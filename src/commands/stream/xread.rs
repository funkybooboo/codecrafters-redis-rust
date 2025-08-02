use crate::commands::Context;
use crate::rdb::{StreamEntry, Value};
use crate::resp::{encode_bulk_resp_string, encode_resp_array, encode_resp_error};
use std::io;
use std::thread;
use std::time::{Duration, Instant};

pub fn cmd_xread(args: &[String], ctx: &mut Context) -> io::Result<Vec<u8>> {
    println!("[cmd_xread] called with args: {:?}", args);

    let mut idx = 1;
    let block_ms = if args.get(idx).map(|s| s.to_lowercase()) == Some("block".into()) {
        let ms = args.get(idx + 1).and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
        println!("[cmd_xread] BLOCK detected: {}ms", ms);
        idx += 2;
        Some(ms)
    } else {
        None
    };

    if args.get(idx).map(|s| s.to_lowercase()) != Some("streams".into()) {
        return Ok(encode_resp_error("usage: XREAD [BLOCK <ms>] STREAMS <key> [<key> ...] <id> [<id> ...]"));
    }
    idx += 1;

    let rem = args.len() - idx;
    if rem < 2 || rem % 2 != 0 {
        return Ok(encode_resp_error("usage: XREAD [BLOCK <ms>] STREAMS <key> [<key> ...] <id> [<id> ...]"));
    }

    let n_streams = rem / 2;
    let keys = &args[idx..idx + n_streams];
    let starts = &args[idx + n_streams..];

    let mut start_positions = Vec::with_capacity(n_streams);
    {
        let store = ctx.store.lock().unwrap();
        for (key, start_raw) in keys.iter().zip(starts.iter()) {
            let entries = match store.get(key) {
                Some((Value::Stream(v), _)) => v.clone(),
                Some(_) => return Ok(encode_resp_error("WRONGTYPE Operation against a key holding the wrong kind of value")),
                None => Vec::new(),
            };

            let pos = if start_raw == "$" {
                entries.iter().fold((0, 0), |(lm, ls), e| {
                    let mut p = e.id.splitn(2, '-');
                    if let (Some(ms), Some(seq)) = (p.next(), p.next()) {
                        if let (Ok(ms), Ok(seq)) = (ms.parse(), seq.parse()) {
                            if ms > lm || (ms == lm && seq > ls) {
                                return (ms, seq);
                            }
                        }
                    }
                    (lm, ls)
                })
            } else if start_raw.contains('-') {
                let mut p = start_raw.splitn(2, '-');
                let ms = p.next().unwrap_or("0").parse().unwrap_or(0);
                let seq = p.next().unwrap_or("0").parse().unwrap_or(0);
                (ms, seq)
            } else {
                (start_raw.parse().unwrap_or(0), 0)
            };

            start_positions.push(pos);
        }
    }

    let collect = || -> Result<Vec<(String, Vec<StreamEntry>)>, ()> {
        let store = ctx.store.lock().unwrap();
        let mut out = Vec::new();

        for (key, &(start_ms, start_seq)) in keys.iter().zip(start_positions.iter()) {
            let entries = match store.get(key) {
                Some((Value::Stream(v), _)) => v.clone(),
                Some(_) => return Err(()),
                None => Vec::new(),
            };

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

    let mut results = match collect() {
        Ok(r) => r,
        Err(_) => return Ok(encode_resp_error("WRONGTYPE Operation against a key holding the wrong kind of value")),
    };

    if results.is_empty() {
        match block_ms {
            None => return Ok(b"*0\r\n".to_vec()),
            Some(0) => {
                loop {
                    thread::sleep(Duration::from_millis(10));
                    results = match collect() {
                        Ok(r) => r,
                        Err(_) => return Ok(encode_resp_error("WRONGTYPE Operation against a key holding the wrong kind of value")),
                    };
                    if !results.is_empty() {
                        break;
                    }
                }
            }
            Some(ms) => {
                let deadline = Instant::now() + Duration::from_millis(ms);
                while Instant::now() < deadline {
                    thread::sleep(Duration::from_millis(10));
                    results = match collect() {
                        Ok(r) => r,
                        Err(_) => return Ok(encode_resp_error("WRONGTYPE Operation against a key holding the wrong kind of value")),
                    };
                    if !results.is_empty() {
                        break;
                    }
                }
                if results.is_empty() {
                    return Ok(b"$-1\r\n".to_vec()); // null bulk
                }
            }
        }
    }

    // Encode result
    let mut outer = Vec::new();
    for (key, entries) in results {
        let mut stream_data = vec![encode_bulk_resp_string(&key)];

        let mut entry_arrs = Vec::with_capacity(entries.len());
        for entry in entries {
            let mut fields = Vec::with_capacity(entry.fields.len() * 2);
            for (k, v) in entry.fields {
                fields.push(encode_bulk_resp_string(&k));
                fields.push(encode_bulk_resp_string(&v));
            }

            let entry_row = vec![
                encode_bulk_resp_string(&entry.id),
                encode_resp_array(&fields),
            ];
            entry_arrs.push(encode_resp_array(&entry_row));
        }

        stream_data.push(encode_resp_array(&entry_arrs));
        outer.push(encode_resp_array(&stream_data));
    }

    Ok(encode_resp_array(&outer))
}
