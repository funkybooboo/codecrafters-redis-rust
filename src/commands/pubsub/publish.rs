use std::io;
use crate::context::Context;
use crate::resp::{encode_int, encode_resp_error};

/// PUBLISH <channel> <message>
/// Returns: integer number of clients that are currently subscribed to channel
pub fn cmd_publish(args: &[String], ctx: &mut Context) -> io::Result<Vec<u8>> {
    if args.len() != 3 {
        return Ok(encode_resp_error(
            "ERR wrong number of arguments for 'publish' command",
        ));
    }
    let channel = &args[1];
    // let message = &args[2]; // we'll deliver this later

    // Count how many subscribers are registered for this channel
    let registry = ctx.pubsub.lock().unwrap();
    let n = registry.get(channel).map_or(0, |subs| subs.len());

    Ok(encode_int(n as i64))
}