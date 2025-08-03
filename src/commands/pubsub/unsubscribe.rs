use crate::Context;
use std::io;

/// UNSUBSCRIBE <channel>
/// Returns: ["unsubscribe", channel, remaining_count]
pub fn cmd_unsubscribe(args: &[String], ctx: &mut Context) -> io::Result<Vec<u8>> {
    // Exactly one argument: the channel to drop
    if args.len() != 2 {
        let err = "-ERR wrong number of arguments for 'unsubscribe'\r\n";
        return Ok(err.as_bytes().to_vec());
    }
    let channel = &args[1];

    // Remove from this client's set (no-op if not present)
    ctx.subscribed_channels.remove(channel);

    // Remove from global registry
    if let Some(this_stream) = ctx.this_client.as_ref() {
        let peer = this_stream.peer_addr().ok();
        let mut registry = ctx.pubsub.lock().unwrap();
        if let Some(subs) = registry.get_mut(channel) {
            subs.retain(|s| s.peer_addr().ok() != peer);
            if subs.is_empty() {
                registry.remove(channel);
            }
        }
    }

    // Build the RESP reply
    let remaining = ctx.subscribed_channels.len();
    let mut resp = Vec::new();
    resp.extend_from_slice(b"*3\r\n");
    resp.extend_from_slice(b"$11\r\nunsubscribe\r\n");
    resp.extend_from_slice(format!("${}\r\n", channel.len()).as_bytes());
    resp.extend_from_slice(channel.as_bytes());
    resp.extend_from_slice(b"\r\n");
    resp.extend_from_slice(format!(":{}\r\n", remaining).as_bytes());
    Ok(resp)
}
