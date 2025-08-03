use crate::Context;
use crate::resp::encode_resp_error;
use std::io;

/// SUBSCRIBE <channel>
/// Returns: ["subscribe", channel, count]
pub fn cmd_subscribe(args: &[String], ctx: &mut Context) -> io::Result<Vec<u8>> {
    if args.len() != 2 {
        return Ok(encode_resp_error(
            "ERR wrong number of arguments for 'subscribe' command",
        ));
    }
    let channel = &args[1];

    // Track per-client subscriptions (avoid duplicates)
    let first = ctx.subscribed_channels.insert(channel.clone());
    if first {
        // First time subscribing → register in global pubsub registry
        if let Some(stream) = ctx.this_client.as_ref() {
            let subscriber = stream.try_clone()?;
            let mut registry = ctx.pubsub.lock().unwrap();
            registry
                .entry(channel.clone())
                .or_insert_with(Vec::new)
                .push(subscriber);
        }
    }

    let count = ctx.subscribed_channels.len();
    let mut resp = Vec::new();
    resp.extend_from_slice(b"*3\r\n$9\r\nsubscribe\r\n");
    resp.extend_from_slice(format!("${}\r\n", channel.len()).as_bytes());
    resp.extend_from_slice(channel.as_bytes());
    resp.extend_from_slice(b"\r\n");
    resp.extend_from_slice(format!(":{}\r\n", count).as_bytes());
    Ok(resp)
}
