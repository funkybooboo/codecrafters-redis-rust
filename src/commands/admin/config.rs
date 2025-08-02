use crate::commands::Context;
use crate::resp::{encode_bulk_resp_string, encode_resp_array, encode_resp_error};
use std::io;

/// CONFIG GET <dir|dbfilename>
pub fn cmd_config(args: &[String], ctx: &mut Context) -> io::Result<Vec<u8>> {
    println!("[cmd_config] Received CONFIG command with args: {:?}", args);

    if args.len() != 3 || args[1].to_uppercase() != "GET" {
        println!("[cmd_config] Incorrect argument length or subcommand");
        return Ok(encode_resp_error("usage: CONFIG GET <dir|dbfilename>"));
    }

    let key = &args[2];
    println!("[cmd_config] Requested config key: '{}'", key);

    let val = match key.as_str() {
        "dir" => {
            println!("[cmd_config] Returning value for 'dir': {}", ctx.cfg.dir);
            &ctx.cfg.dir
        }
        "dbfilename" => {
            println!("[cmd_config] Returning value for 'dbfilename': {}", ctx.cfg.dbfilename);
            &ctx.cfg.dbfilename
        }
        _ => {
            eprintln!("[cmd_config] Unknown config parameter: '{}'", key);
            return Ok(encode_resp_error("unknown config parameter"));
        }
    };

    let response = encode_resp_array(&[
        encode_bulk_resp_string(key),
        encode_bulk_resp_string(val),
    ]);

    Ok(response)
}
