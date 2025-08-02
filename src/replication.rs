use crate::commands::replay_cmd;
use crate::config::ServerConfig;
use crate::resp::{peek_resp_command_size, read_resp_array, write_resp_array};
use crate::Context;
use std::{io, thread};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

pub fn replica_handshake(cfg: &ServerConfig) -> io::Result<TcpStream> {
    println!("[replica_handshake] Connecting to master at {}:{}", cfg.master_host, cfg.master_port);
    let mut master = TcpStream::connect((&cfg.master_host[..], cfg.master_port))?;
    let mut reader = BufReader::new(master.try_clone()?);

    println!("[replica_handshake] Sending PING");
    write_resp_array(&mut master, &["PING"])?;
    let response = wait_for_it(&mut reader)?;
    println!("[replica_handshake] Received response: {}", response.trim());

    println!("[replica_handshake] Sending REPLCONF listening-port {}", cfg.port);
    write_resp_array(
        &mut master,
        &["REPLCONF", "listening-port", &cfg.port.to_string()],
    )?;
    let response = wait_for_it(&mut reader)?;
    println!("[replica_handshake] Received response: {}", response.trim());

    println!("[replica_handshake] Sending REPLCONF capa psync2");
    write_resp_array(&mut master, &["REPLCONF", "capa", "psync2"])?;
    let response = wait_for_it(&mut reader)?;
    println!("[replica_handshake] Received response: {}", response.trim());

    println!("[replica_handshake] Sending PSYNC ? -1");
    write_resp_array(&mut master, &["PSYNC", "?", "-1"])?;
    let response = wait_for_it(&mut reader)?;
    println!("[replica_handshake] Received response: {}", response.trim());

    let mut rdb_header = String::new();
    reader.read_line(&mut rdb_header)?;
    let rdb_header = rdb_header.trim_end();
    println!("[replica_handshake] RDB header received: {}", rdb_header);

    if !rdb_header.starts_with('$') {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Expected RDB length header"));
    }

    let rdb_len: usize = rdb_header[1..]
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid RDB length"))?;
    println!("[replica_handshake] Reading RDB snapshot of length: {}", rdb_len);

    let mut rdb_buf = vec![0u8; rdb_len];
    reader.read_exact(&mut rdb_buf)?;
    println!("[replica_handshake] RDB snapshot successfully read.");

    Ok(master)
}

fn wait_for_it(reader: &mut dyn BufRead) -> io::Result<String> {
    let mut line = String::new();
    reader.read_line(&mut line)?;
    println!("[wait_for_it] Received line: {}", line.trim());
    Ok(line)
}

pub fn replication_loop(stream: TcpStream, mut ctx: Context) -> io::Result<()> {
    println!("[replication_loop] Starting replication loop...");
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = stream;

    loop {
        println!("[replication_loop] Peeking next command size...");
        let cmd_size = match peek_resp_command_size(&mut reader) {
            Ok(0) => {
                // incomplete, wait and retry
                thread::sleep(Duration::from_millis(1));
                continue;
            }
            Ok(n) => n,
            Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                println!("[replication_loop] EOF received, ending loop.");
                break;
            }
            Err(e) => {
                eprintln!("[replication_loop] peek error: {}", e);
                break;
            }
        };

        let offset_before = ctx.master_repl_offset;

        println!("[replication_loop] Reading next command...");
        let maybe_args = read_resp_array(&mut reader);
        match maybe_args {
            Err(e) => {
                eprintln!("[replication_loop] Error reading command: {}", e);
                break;
            }
            Ok(None) => {
                println!("[replication_loop] No more commands, ending loop.");
                break;
            }
            Ok(Some(args)) => {
                if args.is_empty() {
                    println!("[replication_loop] Empty command received, skipping.");
                    continue;
                }

                let cmd = args[0].to_uppercase();
                println!("[replication_loop] Received command: {} {:?}", cmd, &args[1..]);

                if cmd == "REPLCONF"
                    && args.len() == 3
                    && args[1].to_uppercase() == "GETACK"
                    && args[2] == "*"
                {
                    println!(
                        "[replication_loop] Handling REPLCONF GETACK request. Returning offset {} (before {} byte command)",
                        offset_before, cmd_size
                    );
                    write_resp_array(&mut writer, &["REPLCONF", "ACK", &offset_before.to_string()])?;
                    writer.flush()?;
                    println!("[replication_loop] ACK sent with offset {}", offset_before);
                } else {
                    println!("[replication_loop] Replaying command: {}", cmd);
                    replay_cmd(&cmd, &mut writer, &args, &mut ctx)?;
                    writer.flush()?;
                    println!("[replication_loop] Command replayed successfully: {}", cmd);
                }

                ctx.master_repl_offset += cmd_size;
                println!(
                    "[replication_loop] Master replication offset updated to {}",
                    ctx.master_repl_offset
                );
            }
        }
    }

    println!("[replication_loop] Replication loop ended.");
    Ok(())
}
