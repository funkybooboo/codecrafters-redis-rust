use crate::commands::Context;
use crate::rdb::{StreamEntry, Value};
use crate::resp::{encode_bulk_resp_string, encode_resp_array, encode_resp_error};
use std::io;

pub fn cmd_xrange(args: &[String], ctx: &mut Context) -> io::Result<Vec<u8>> {
    println!("[cmd_xrange] Received XRANGE command: {:?}", args);

    if args.len() != 4 {
        println!("[cmd_xrange] Invalid number of arguments");
        return Ok(encode_resp_error("usage: XRANGE <key> <start> <end>"));
    }

    let key = &args[1];
    let start_raw = &args[2];
    let end_raw = &args[3];

    let entries: Vec<StreamEntry> = {
        let map = ctx.store.lock().unwrap();
        match map.get(key) {
            None => return Ok(b"*0\r\n".to_vec()),
            Some((Value::Stream(v), _)) => v.clone(),
            Some(_) => {
                return Ok(encode_resp_error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                ));
            }
        }
    };

    fn parse_start(raw: &str) -> Result<(u64, u64), ()> {
        if raw == "-" {
            Ok((0, 0))
        } else if raw.contains('-') {
            let mut p = raw.splitn(2, '-');
            let ms = p.next().unwrap().parse().map_err(|_| ())?;
            let seq = p.next().unwrap().parse().map_err(|_| ())?;
            Ok((ms, seq))
        } else {
            let ms = raw.parse().map_err(|_| ())?;
            Ok((ms, 0))
        }
    }

    fn parse_end(raw: &str, entries: &[StreamEntry]) -> Result<(u64, u64), ()> {
        if raw == "+" {
            Ok((u64::MAX, u64::MAX))
        } else if raw.contains('-') {
            let mut p = raw.splitn(2, '-');
            let ms = p.next().unwrap().parse().map_err(|_| ())?;
            let seq = p.next().unwrap().parse().map_err(|_| ())?;
            Ok((ms, seq))
        } else {
            let ms = raw.parse().map_err(|_| ())?;
            let max_seq = entries
                .iter()
                .filter_map(|e| {
                    let mut p = e.id.splitn(2, '-');
                    let t = p.next()?.parse::<u64>().ok()?;
                    let s = p.next()?.parse::<u64>().ok()?;
                    if t == ms { Some(s) } else { None }
                })
                .max()
                .unwrap_or(0);
            Ok((ms, max_seq))
        }
    }

    let (start_ms, start_seq) = match parse_start(start_raw) {
        Ok(x) => x,
        Err(_) => return Ok(encode_resp_error("ERR invalid start ID format")),
    };

    let (end_ms, end_seq) = match parse_end(end_raw, &entries) {
        Ok(x) => x,
        Err(_) => return Ok(encode_resp_error("ERR invalid end ID format")),
    };

    let filtered: Vec<StreamEntry> = entries
        .into_iter()
        .filter(|e| {
            let mut p = e.id.splitn(2, '-');
            let ems = p.next().and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
            let eseq = p.next().and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
            (ems > start_ms || (ems == start_ms && eseq >= start_seq))
                && (ems < end_ms || (ems == end_ms && eseq <= end_seq))
        })
        .collect();

    let mut outer = Vec::with_capacity(filtered.len());
    for entry in filtered {
        let mut inner = vec![encode_bulk_resp_string(&entry.id)];
        let mut kv_array = Vec::with_capacity(entry.fields.len() * 2);
        for (k, v) in entry.fields {
            kv_array.push(encode_bulk_resp_string(&k));
            kv_array.push(encode_bulk_resp_string(&v));
        }
        inner.push(encode_resp_array(&kv_array));
        outer.push(encode_resp_array(&inner));
    }

    Ok(encode_resp_array(&outer))
}
