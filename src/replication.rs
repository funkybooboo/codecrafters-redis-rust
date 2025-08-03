use crate::commands::dispatch_cmd;
use crate::config::ServerConfig;
use crate::resp::{peek_resp_command_size, read_resp_array, write_resp_array};
use crate::Context;

use crate::rdb::load_rdb_snapshot_from_stream;
use std::io::{self, BufRead, BufReader, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

pub fn connect_and_sync_master(mut ctx: Context) -> io::Result<()> {
    println!("[replication::main] Beginning full replication process...");

    let mut stream = connect_to_master(&ctx.cfg)?;
    ctx.this_client = Some(stream.try_clone()?);
    println!("[replication::main] Connected to master.");

    let mut reader = BufReader::new(stream.try_clone()?);
    println!("[replication::main] Cloned stream for reading.");

    perform_handshake(&mut stream, &mut reader, &ctx.cfg)?;
    println!("[replication::main] Handshake complete.");

    load_rdb_snapshot_from_stream(&mut reader, &mut ctx)?;
    println!("[replication::main] RDB snapshot loaded.");

    stream_command_loop(&mut reader, &mut stream, &mut ctx)?;
    println!("[replication::main] Command streaming loop exited.");

    Ok(())
}

fn connect_to_master(cfg: &ServerConfig) -> io::Result<TcpStream> {
    println!("[replication::connect] Connecting to {}:{}", cfg.master_host, cfg.master_port);
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
    println!("[replication::handshake] Sent {:?}, received: {}", cmd, line.trim());

    if !line.starts_with(expected_prefix) {
        return Err(io::Error::new(io::ErrorKind::InvalidData, format!("Expected {}", expected_prefix)));
    }

    Ok(())
}

pub fn stream_command_loop(
    reader: &mut BufReader<TcpStream>,
    writer: &mut TcpStream,
    ctx: &mut Context,
) -> io::Result<()> {
    println!("[replication::stream] Entered command loop.");

    loop {
        let cmd_size = match peek_resp_command_size(reader) {
            Ok(0) => {
                thread::sleep(Duration::from_millis(1));
                continue;
            }
            Ok(n) => n,
            Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                println!("[replication::stream] Master disconnected (EOF).");
                break;
            }
            Err(e) => {
                eprintln!("[replication::stream] Peek error: {}", e);
                break;
            }
        };

        let maybe_args = read_resp_array(reader);
        match maybe_args {
            Err(e) => {
                eprintln!("[replication::stream] Error reading command: {}", e);
                break;
            }
            Ok(None) => {
                println!("[replication::stream] No more commands.");
                break;
            }
            Ok(Some(args)) => {
                if args.is_empty() {
                    println!("[replication::stream] Empty command.");
                    continue;
                }

                let cmd = args[0].to_uppercase();
                println!("[replication::stream] Dispatching command: {:?}", args);

                dispatch_cmd(&cmd, writer, &args, ctx)?;
                writer.flush()?;

                ctx.master_repl_offset += cmd_size;
                println!(
                    "[replication::stream] Updated master_repl_offset: {}",
                    ctx.master_repl_offset
                );
            }
        }
    }

    println!("[replication::stream] Exiting command loop.");
    Ok(())
}
