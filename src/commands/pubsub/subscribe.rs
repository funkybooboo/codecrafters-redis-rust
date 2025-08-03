use crate::context::Context;
use crate::resp::{encode_resp_error, encode_int};
use std::io;

pub fn cmd_subscribe(args: &[String], ctx: &mut Context) -> io::Result<Vec<u8>> {
    // Must be exactly: SUBSCRIBE <channel>
    if args.len() != 2 {
        return Ok(encode_resp_error("usage: SUBSCRIBE <channel>"));
    }
    let channel = &args[1];

    // Grab a clone of the current client stream
    let subscriber = ctx.this_client
        .as_ref()
        .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "No client to subscribe"))?
        .try_clone()?;

    // Register them
    let mut subs = ctx.sub_handlers.lock().unwrap();
    let entry = subs.entry(channel.clone()).or_insert_with(Vec::new);
    entry.push(subscriber);
    let count = entry.len() as i64;

    // Build the RESP array: ["subscribe", channel, count]
    let mut resp = Vec::new();
    resp.extend_from_slice(b"*3\r\n");
    resp.extend_from_slice(b"$9\r\nsubscribe\r\n");
    resp.extend_from_slice(format!("${}\r\n{}\r\n", channel.len(), channel).as_bytes());
    resp.extend_from_slice(encode_int(count).as_slice());
    Ok(resp)
}
