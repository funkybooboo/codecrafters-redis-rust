use crate::commands::Context;
use crate::rdb::{StreamEntry, Value};
use crate::resp::{encode_bulk_resp_string, encode_resp_error};
use std::io;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn cmd_xadd(args: &[String], ctx: &mut Context) -> io::Result<Vec<u8>> {
    println!("[cmd_xadd] Received XADD command with args: {:?}", args);

    if args.len() < 5 || (args.len() - 3) % 2 != 0 {
        println!("[cmd_xadd] Invalid number of arguments.");
        return Ok(encode_resp_error(
            "usage: XADD <key> <id> <field> <value> [<field> <value> ...]",
        ));
    }

    let key = &args[1];
    let id_raw = &args[2];

    println!("[cmd_xadd] Target stream key: '{}', Raw ID: '{}'", key, id_raw);

    let (ms, seq, final_id) = if id_raw == "*" {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).expect("Clock error");
        let ms = now.as_millis() as u64;

        let max_seq = {
            let map = ctx.store.lock().unwrap();
            map.get(key).and_then(|pair| {
                if let Value::Stream(entries) = &pair.0 {
                    entries
                        .iter()
                        .filter_map(|e| {
                            let mut p = e.id.splitn(2, '-');
                            let t = p.next()?.parse::<u64>().ok()?;
                            let s = p.next()?.parse::<u64>().ok()?;
                            if t == ms { Some(s) } else { None }
                        })
                        .max()
                } else {
                    None
                }
            })
        };

        let seq = max_seq.map_or(0, |n| n + 1);
        println!("[cmd_xadd] Auto-generated ID: {}-{}", ms, seq);
        (ms, seq, format!("{ms}-{seq}"))
    } else if let Some(ms_str) = id_raw.strip_suffix("-*") {
        let ms = match ms_str.parse::<u64>() {
            Ok(n) => n,
            Err(_) => {
                eprintln!("[cmd_xadd] Invalid milliseconds in ID suffix: '{}'", ms_str);
                return Ok(encode_resp_error("The ID specified in XADD has invalid format"));
            }
        };

        let max_seq = {
            let map = ctx.store.lock().unwrap();
            map.get(key).and_then(|pair| {
                if let Value::Stream(entries) = &pair.0 {
                    entries
                        .iter()
                        .filter_map(|e| {
                            let mut p = e.id.splitn(2, '-');
                            let t = p.next()?.parse::<u64>().ok()?;
                            let s = p.next()?.parse::<u64>().ok()?;
                            if t == ms { Some(s) } else { None }
                        })
                        .max()
                } else {
                    None
                }
            })
        };

        let base = if ms == 0 { 1 } else { 0 };
        let seq = max_seq.map_or(base, |n| n + 1);
        println!("[cmd_xadd] Auto-generated sequence for ms={}: {}", ms, seq);
        (ms, seq, format!("{ms}-{seq}"))
    } else {
        let mut parts = id_raw.splitn(2, '-');
        let ms = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
        let seq = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
        println!("[cmd_xadd] Using manual ID: {}-{}", ms, seq);
        (ms, seq, id_raw.clone())
    };

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

        if ms < last_ms || (ms == last_ms && seq <= last_seq) {
            let msg = if ms == 0 && seq == 0 {
                "The ID specified in XADD must be greater than 0-0"
            } else {
                "The ID specified in XADD is equal or smaller than the target stream top item"
            };
            eprintln!(
                "[cmd_xadd] Monotonicity error: last={} last_seq={}, new={} new_seq={}",
                last_ms, last_seq, ms, seq
            );
            return Ok(encode_resp_error(msg));
        }
    }

    let fields: Vec<(String, String)> = args[3..]
        .chunks(2)
        .map(|chunk| (chunk[0].clone(), chunk[1].clone()))
        .collect();

    println!("[cmd_xadd] Parsed {} field-value pair(s)", fields.len());

    let mut map = ctx.store.lock().unwrap();
    match map.get_mut(key) {
        Some((Value::Stream(ref mut entries), _)) => {
            println!("[cmd_xadd] Appending entry to existing stream at key '{}'", key);
            entries.push(StreamEntry {
                id: final_id.clone(),
                fields,
            });
        }
        Some(_) => {
            eprintln!("[cmd_xadd] WRONGTYPE: Key '{}' exists but is not a stream", key);
            return Ok(encode_resp_error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            ));
        }
        None => {
            println!("[cmd_xadd] Creating new stream at key '{}'", key);
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

    println!("[cmd_xadd] Successfully added entry with ID: {}", final_id);
    Ok(encode_bulk_resp_string(&final_id))
}
