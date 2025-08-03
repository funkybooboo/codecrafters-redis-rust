use crate::commands::Context;
use crate::resp::{encode_simple_resp_string, encode_resp_error, write_resp_array};
use std::io::{self, Write};
use std::net::SocketAddr;

/// REPLCONF <option> <value>
pub fn cmd_replconf(args: &[String], ctx: &mut Context) -> io::Result<Vec<u8>> {
    let peer = ctx
        .this_client
        .as_ref()
        .and_then(|s| s.peer_addr().ok())
        .unwrap_or_else(|| "unknown address".parse().unwrap());

    println!("[cmd_replconf] Received REPLCONF from {:?} with args: {:?}", peer, args);

    if args.len() < 3 {
        eprintln!("[cmd_replconf] Invalid number of arguments: expected 3, got {}", args.len());
        return Ok(encode_resp_error("usage: REPLCONF <option> <value>"));
    }

    let option = &args[1];
    let value = &args[2];
    println!("[cmd_replconf] Parsed Option: '{}', Value: '{}'", option, value);

    match option.to_ascii_lowercase().as_str() {
        "listening-port" => {
            println!("[cmd_replconf] Peer reports listening port: {}", value);
            Ok(encode_simple_resp_string("OK"))
        }
        "capa" => {
            println!("[cmd_replconf] Capability reported: {}", value);
            Ok(encode_simple_resp_string("OK"))
        }
        "ack" => {
            if let Ok(offset) = value.parse::<usize>() {
                if let Some(ref stream) = ctx.this_client {
                    let peer =stream.peer_addr().unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)));
                    if let Some(replica) = ctx.replicas.lock().unwrap().get_mut(&peer) {
                        replica.1 = offset;
                        println!("[cmd_replconf] Updated replica offset: {:?} -> {}", peer, offset);
                    } else {
                        println!("[cmd_replconf] ACK received from unregistered replica: {:?}", peer);
                    }
                }
            } else {
                eprintln!("[cmd_replconf] Invalid ACK offset: '{}'", value);
            }
            Ok(encode_simple_resp_string("OK"))
        }
        "getack" if value == "*" => {
            println!("[cmd_replconf] GETACK received - replying with current offset");
            if let Some(ref mut stream) = ctx.this_client {
                let ack_value = ctx.master_repl_offset.to_string();
                write_resp_array(stream, &["REPLCONF", "ACK", &ack_value])?;
                stream.flush()?;
                println!("[cmd_replconf] Sent: REPLCONF ACK {}", ack_value);
            } else {
                eprintln!("[cmd_replconf] No stream available to reply with ACK");
            }
            Ok(vec![]) // No RESP reply needed, we already wrote directly
        }
        _ => {
            println!("[cmd_replconf] Unhandled REPLCONF option: '{}'", option);
            Ok(encode_simple_resp_string("OK"))
        }
    }
}
