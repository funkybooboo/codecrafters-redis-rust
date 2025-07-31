use crate::commands::Context;
use crate::rdb::{StreamEntry, Value};
use crate::resp::{write_bulk_string, write_error};
use std::io;
use std::net::TcpStream;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn cmd_xadd(out: &mut TcpStream, args: &[String], ctx: &mut Context) -> io::Result<()> {
    // 1) Validate argument count
    if args.len() < 5 || (args.len() - 3) % 2 != 0 {
        write_error(
            out,
            "usage: XADD <key> <id> <field> <value> [<field> <value> ...]",
        )?;
        return Ok(());
    }

    let key = &args[1];
    let id_raw = &args[2];

    // 2) Compute (ms, seq, final_id) based on id_raw
    let (ms, seq, final_id) = if id_raw == "*" {
        // Auto‐generate both time and sequence
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before UNIX EPOCH");
        let ms = now.as_millis() as u64;

        // Find max seq for this ms
        let max_seq = {
            let map = ctx.store.lock().unwrap();
            map.get(key).and_then(|pair| {
                if let Value::Stream(entries) = &pair.0 {
                    entries
                        .iter()
                        .filter_map(|e| {
                            let mut p = e.id.splitn(2, '-');
                            let t = p.next().and_then(|s| s.parse::<u64>().ok())?;
                            let s = p.next().and_then(|s| s.parse::<u64>().ok())?;
                            if t == ms {
                                Some(s)
                            } else {
                                None
                            }
                        })
                        .max()
                } else {
                    None
                }
            })
        };

        let seq = max_seq.map(|n| n + 1).unwrap_or(0);
        (ms, seq, format!("{ms}-{seq}"))
    } else if let Some(ms_str) = id_raw.strip_suffix("-*") {
        // Auto‐generate sequence only
        let ms = match ms_str.parse::<u64>() {
            Ok(n) => n,
            Err(_) => {
                write_error(out, "The ID specified in XADD has invalid format")?;
                return Ok(());
            }
        };

        // Find max seq for this ms
        let max_seq = {
            let map = ctx.store.lock().unwrap();
            map.get(key).and_then(|pair| {
                if let Value::Stream(entries) = &pair.0 {
                    entries
                        .iter()
                        .filter_map(|e| {
                            let mut p = e.id.splitn(2, '-');
                            let t = p.next().and_then(|s| s.parse::<u64>().ok())?;
                            let s = p.next().and_then(|s| s.parse::<u64>().ok())?;
                            if t == ms {
                                Some(s)
                            } else {
                                None
                            }
                        })
                        .max()
                } else {
                    None
                }
            })
        };

        // Default base sequence: 1 when ms==0, else 0
        let base = if ms == 0 { 1 } else { 0 };
        let seq = max_seq.map(|n| n + 1).unwrap_or(base);
        (ms, seq, format!("{ms}-{seq}"))
    } else {
        // Explicit "<ms>-<seq>"
        let mut parts = id_raw.splitn(2, '-');
        let ms = match parts.next().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => {
                write_error(out, "The ID specified in XADD has invalid format")?;
                return Ok(());
            }
        };
        let seq = match parts.next().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => {
                write_error(out, "The ID specified in XADD has invalid format")?;
                return Ok(());
            }
        };
        (ms, seq, id_raw.clone())
    };

    // 3) If explicit, enforce monotonic ordering
    if !id_raw.ends_with("-*") && id_raw != "*" {
        let (last_ms, last_seq) = {
            let map = ctx.store.lock().unwrap();
            if let Some(pair) = map.get(key) {
                if let Value::Stream(entries) = &pair.0 {
                    if let Some(last) = entries.last() {
                        let mut p = last.id.splitn(2, '-');
                        let lms = p.next().unwrap().parse::<u64>().unwrap_or(0);
                        let lseq = p.next().unwrap().parse::<u64>().unwrap_or(0);
                        (lms, lseq)
                    } else {
                        (0, 0)
                    }
                } else {
                    (0, 0)
                }
            } else {
                (0, 0)
            }
        };

        // Special minimum check for 0-0
        let is_minimum = ms == 0 && seq == 0;
        if ms < last_ms || (ms == last_ms && seq <= last_seq) {
            if is_minimum {
                write_error(out, "The ID specified in XADD must be greater than 0-0")?;
            } else {
                write_error(
                    out,
                    "The ID specified in XADD is equal or smaller than the target stream top item",
                )?;
            }
            return Ok(());
        }
    }

    // 4) Build fields and insert entry
    let fields: Vec<(String, String)> = args[3..]
        .chunks(2)
        .map(|chunk| (chunk[0].clone(), chunk[1].clone()))
        .collect();

    let mut map = ctx.store.lock().unwrap();
    match map.get_mut(key) {
        Some((Value::Stream(ref mut entries), _)) => {
            entries.push(StreamEntry {
                id: final_id.clone(),
                fields,
            });
        }
        Some(_) => {
            write_error(
                out,
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )?;
            return Ok(());
        }
        None => {
            map.insert(
                key.clone(),
                (
                    Value::Stream(vec![StreamEntry {
                        id: final_id.clone(),
                        fields,
                    }]),
                    None,
                ),
            );
        }
    }

    // 5) Reply with the chosen ID
    write_bulk_string(out, &final_id)
}
