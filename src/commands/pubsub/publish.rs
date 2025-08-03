use crate::Context;
use crate::resp::{encode_int, write_resp_array};
use std::io;
use std::io::Write;

/// PUBLISH <channel> <message>
/// Reply: (integer) number of subscribers the message was delivered to
pub fn cmd_publish(args: &[String], ctx: &mut Context) -> io::Result<Vec<u8>> {
    if args.len() != 3 {
        // wrong number of args: return a RESP error
        return Ok(b"-ERR wrong number of arguments for 'publish'\r\n".to_vec());
    }
    let channel = &args[1];
    let message = &args[2];

    // Take the lock once
    let mut registry = ctx.pubsub.lock().unwrap();
    let subs = registry.get(channel).map(|v| v.len()).unwrap_or(0);

    // Deliver the message to each subscriber
    if let Some(subscribers) = registry.get_mut(channel) {
        for subscriber in subscribers.iter_mut() {
            // ["message", channel, message]
            let _ = write_resp_array(
                subscriber,
                &["message", channel.as_str(), message.as_str()],
            ).and_then(|_| subscriber.flush());
        }
    }

    // Reply with the number of clients we delivered to
    Ok(encode_int(subs as i64))
}
