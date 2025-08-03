use crate::commands::Context;
use crate::resp::{encode_simple_resp_string, encode_resp_error};
use std::io;

/// REPLCONF <option> <value>
pub fn cmd_replconf(args: &[String], ctx: &mut Context) -> io::Result<Vec<u8>> {
    println!(
        "[cmd_replconf] Received REPLCONF command from {:?} with args: {:?}",
        ctx.this_client
            .as_ref()
            .and_then(|s| s.peer_addr().ok())
            .unwrap_or_else(|| "unknown address".parse().unwrap()),
        args
    );

    if args.len() < 3 {
        eprintln!("[cmd_replconf] Invalid number of arguments: expected 3, got {}", args.len());
        return Ok(encode_resp_error("usage: REPLCONF <option> <value>"));
    }

    let option = &args[1];
    let value = &args[2];
    println!("[cmd_replconf] Parsed Option: '{}', Value: '{}'", option, value);

    // Optional: act on known options if desired
    match option.to_ascii_lowercase().as_str() {
        "listening-port" => {
            println!("[cmd_replconf] Peer reports listening port: {}", value);
        }
        "capa" => {
            println!("[cmd_replconf] Capability reported: {}", value);
        }
        _ => {
            println!("[cmd_replconf] Unhandled REPLCONF option: '{}'", option);
        }
    }

    Ok(encode_simple_resp_string("OK"))
}
