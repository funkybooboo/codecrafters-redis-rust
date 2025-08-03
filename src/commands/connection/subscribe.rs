use crate::Context;
use std::io;

/// SUBSCRIBE <channel>
/// Returns: ["subscribe", channel, count]
pub fn cmd_subscribe(args: &[String], ctx: &mut Context) -> io::Result<Vec<u8>> {
    // Exactly one argument: the channel name
    if args.len() != 2 {
        let err = "-ERR wrong number of arguments for 'subscribe' command\r\n";
        return Ok(err.as_bytes().to_vec());
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

    // The total number of distinct channels this client is subscribed to
    let count = ctx.subscribed_channels.len();

    // Build the RESP reply: *3, $9 subscribe, $<len> <channel>, :<count>
    let mut resp = Vec::new();
    resp.extend_from_slice(b"*3\r\n");
    resp.extend_from_slice(b"$9\r\nsubscribe\r\n");
    resp.extend_from_slice(format!("${}\r\n", channel.len()).as_bytes());
    resp.extend_from_slice(channel.as_bytes());
    resp.extend_from_slice(b"\r\n");
    resp.extend_from_slice(format!(":{}\r\n", count).as_bytes());
    Ok(resp)
}
