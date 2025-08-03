use crate::commands::replay_cmd;
use crate::config::ServerConfig;
use crate::resp::{peek_resp_command_size, read_resp_array, write_resp_array};
use crate::Context;

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

pub fn handle_replication(mut ctx: Context) -> io::Result<()> {
    println!("[handle_replication] Beginning full replication process...");

    let mut stream = connect_to_master(&ctx.cfg)?;
    println!("[handle_replication] Connected to master.");

    let mut reader = BufReader::new(stream.try_clone()?);
    println!("[handle_replication] Cloned stream for reading.");

    perform_handshake(&mut stream, &mut reader, &ctx.cfg)?;
    println!("[handle_replication] Handshake complete.");

    load_rdb_snapshot(&mut reader, &mut ctx)?;
    println!("[handle_replication] RDB snapshot loaded.");

    stream_command_loop(&mut reader, &mut stream, &mut ctx)?;
    println!("[handle_replication] Command streaming loop exited.");

    Ok(())
}


// ========== Phase 1: Connect + Handshake ==========

fn connect_to_master(cfg: &ServerConfig) -> io::Result<TcpStream> {
    println!("[connect_to_master] Connecting to {}:{}", cfg.master_host, cfg.master_port);
    TcpStream::connect((&cfg.master_host[..], cfg.master_port))
}

fn perform_handshake(
    stream: &mut TcpStream,
    reader: &mut BufReader<TcpStream>,
    cfg: &ServerConfig,
) -> io::Result<()> {
    send_and_expect(stream, reader, &["PING"], "+PONG")?;
    send_and_expect(stream, reader, &["REPLCONF", "listening-port", &cfg.port.to_string()], "+OK")?;
    send_and_expect(stream, reader, &["REPLCONF", "capa", "psync2"], "+OK")?;
    send_and_expect(stream, reader, &["PSYNC", "?", "-1"], "+FULLRESYNC")?;

    Ok(())
}

fn send_and_expect(
    writer: &mut TcpStream,
    reader: &mut BufReader<TcpStream>,
    cmd: &[&str],
    expected_prefix: &str,
) -> io::Result<()> {
    write_resp_array(writer, cmd)?;
    writer.flush()?;

    let mut line = String::new();
    reader.read_line(&mut line)?;
    println!("[handshake] Sent {:?}, received: {}", cmd, line.trim());

    if !line.starts_with(expected_prefix) {
        return Err(io::Error::new(io::ErrorKind::InvalidData, format!("Expected {}", expected_prefix)));
    }

    Ok(())
}

// ========== Phase 2: Load RDB ==========

fn load_rdb_snapshot(reader: &mut BufReader<TcpStream>, ctx: &mut Context) -> io::Result<()> {
    let mut rdb_header = String::new();
    reader.read_line(&mut rdb_header)?;
    println!("[replication::rdb_load] RDB header: {}", rdb_header.trim());

    if !rdb_header.starts_with('$') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Expected '$' prefix for RDB header, got: '{}'", rdb_header.trim()),
        ));
    }

    let rdb_len: usize = rdb_header[1..].trim().parse().map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidData, "Invalid RDB length in header")
    })?;

    let mut rdb_buf = vec![0; rdb_len];
    reader.read_exact(&mut rdb_buf)?;
    println!("[replication::rdb_load] Snapshot read ({} bytes).", rdb_len);

    let parsed = crate::rdb::parse_rdb_bytes(&rdb_buf)?;
    *ctx.store.lock().unwrap() = parsed;
    println!("[replication::rdb_load] Snapshot loaded into store successfully.");

    Ok(())
}

// ========== Phase 3: Stream Command Loop ==========

fn stream_command_loop(
    reader: &mut BufReader<TcpStream>,
    writer: &mut TcpStream,
    ctx: &mut Context,
) -> io::Result<()> {
    println!("[stream_loop] Entered command loop.");

    loop {
        println!("[stream_loop] Loop tick. Checking for command size...");
        let cmd_size = match peek_resp_command_size(reader) {
            Ok(0) => {
                println!("[stream_loop] No command detected, sleeping...");
                thread::sleep(Duration::from_millis(1));
                continue;
            }
            Ok(n) => {
                println!("[stream_loop] Peeked command size: {n}");
                n
            }
            Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                println!("[stream_loop] Master disconnected (EOF).");
                break;
            }
            Err(e) => {
                eprintln!("[stream_loop] Peek error: {}", e);
                break;
            }
        };

        println!("[stream_loop] Attempting to read RESP array...");
        let maybe_args = read_resp_array(reader);
        match maybe_args {
            Err(e) => {
                eprintln!("[stream_loop] Error reading command: {}", e);
                break;
            }
            Ok(None) => {
                println!("[stream_loop] No more commands.");
                break;
            }
            Ok(Some(args)) => {
                println!("[stream_loop] Received command: {:?}", args);

                if args.is_empty() {
                    println!("[stream_loop] Empty command.");
                    continue;
                }

                let cmd = args[0].to_uppercase();
                if is_getack(&args) {
                    println!("[stream_loop] Detected GETACK command.");
                    let offset = ctx.master_repl_offset;
                    write_resp_array(writer, &["REPLCONF", "ACK", &offset.to_string()])?;
                    writer.flush()?;
                    println!("[stream_loop] Sent GETACK with offset {}", offset);
                } else {
                    println!("[stream_loop] Replaying: {:?}", args);
                    replay_cmd(&cmd, writer, &args, ctx)?;
                    writer.flush()?;
                    println!("[stream_loop] Done replaying.");
                }

                ctx.master_repl_offset += cmd_size;
                println!("[stream_loop] Updated master_repl_offset: {}", ctx.master_repl_offset);
            }
        }
    }

    println!("[stream_loop] Exiting command loop.");
    Ok(())
}

fn is_getack(args: &[String]) -> bool {
    args.len() == 3
        && args[0].eq_ignore_ascii_case("REPLCONF")
        && args[1].eq_ignore_ascii_case("GETACK")
        && args[2] == "*"
}
