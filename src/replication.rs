use crate::commands::replay_cmd;
use crate::config::ServerConfig;
use crate::resp::read_array;
use crate::resp::write_array;
use crate::Context;
use std::io;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;

/// Full replica handshake (parts 1–4):
///
/// 1) PING
///    -> +PONG
/// 2) REPLCONF listening-port <our-port>
///    -> +OK
/// 3) REPLCONF capa psync2
///    -> +OK
/// 4) PSYNC ? -1
///    -> +FULLRESYNC <replid> 0
///    -> $<rdb-len>\r\n<rdb-bytes>
pub fn replica_handshake(cfg: &ServerConfig) -> io::Result<TcpStream> {
    // Connect to the master
    let mut master = TcpStream::connect((&cfg.master_host[..], cfg.master_port))?;
    let mut reader = BufReader::new(master.try_clone()?);

    // 1) Send PING
    write_array(&mut master, &["PING"])?;
    let _ = wait_for_it(&mut reader)?;

    // 2) REPLCONF listening-port
    write_array(
        &mut master,
        &["REPLCONF", "listening-port", &cfg.port.to_string()],
    )?;
    let _ = wait_for_it(&mut reader)?;

    // 3) REPLCONF capa psync2
    write_array(&mut master, &["REPLCONF", "capa", "psync2"])?;
    let _ = wait_for_it(&mut reader)?;

    // 4) PSYNC ? -1
    write_array(&mut master, &["PSYNC", "?", "-1"])?;
    let fullresync_line = wait_for_it(&mut reader)?; // +FULLRESYNC <replid> <offset>
    println!("[replica_handshake] FULLRESYNC line: {}", fullresync_line.trim_end());

    // Read: $<rdb-len>\r\n
    let mut rdb_header = String::new();
    reader.read_line(&mut rdb_header)?;
    let rdb_header = rdb_header.trim_end();
    if !rdb_header.starts_with('$') {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Expected RDB length header"));
    }

    let rdb_len: usize = rdb_header[1..]
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid RDB length"))?;
    println!("[replica_handshake] Receiving RDB file of {} bytes…", rdb_len);

    // Read and discard the binary RDB file
    let mut rdb_buf = vec![0u8; rdb_len];
    reader.read_exact(&mut rdb_buf)?;
    println!("[replica_handshake] RDB received and discarded.");

    Ok(master)
}

/// Reads exactly one line (`+\r\n`, `-ERR…\r\n`, etc.) from the master
/// and returns it (including the trailing CRLF).
fn wait_for_it(reader: &mut dyn BufRead) -> io::Result<String> {
    let mut line = String::new();
    reader.read_line(&mut line)?;
    Ok(line)
}

/// Read commands from master, replay only write‐type ones through your normal cmd_*
/// (which mutates ctx.store for you). Any “OK” they emit is ignored.
pub fn replication_loop(stream: TcpStream, mut ctx: Context) -> std::io::Result<()> {
    println!("[replication_loop] 🔄 Started replication loop");

    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = stream;

    loop {
        println!("[replication_loop] ⏳ Waiting for next command from master…");

        let maybe_args = read_array(&mut reader);
        match maybe_args {
            Err(e) => {
                eprintln!("[replication_loop] ❌ Error reading from master: {e}");
                break;
            }

            Ok(None) => {
                println!("[replication_loop] 🔚 EOF from master — exiting loop.");
                break;
            }

            Ok(Some(args)) => {
                println!("[replication_loop] 📥 Received command array: {:?}", args);

                if args.is_empty() {
                    println!("[replication_loop] ⚠️ Empty command array — skipping.");
                    continue;
                }

                let cmd = args[0].to_uppercase();
                println!("[replication_loop] 🧠 Command: {}", cmd);

                // Special-case: REPLCONF GETACK *
                if cmd == "REPLCONF" {
                    println!("[replication_loop] ↪️ REPLCONF detected. Checking subcommand…");

                    if args.len() == 3 {
                        let subcmd = args[1].to_uppercase();
                        let wildcard = &args[2];
                        println!(
                            "[replication_loop] ↪️ Subcommand = {}, Arg3 = {}",
                            subcmd, wildcard
                        );

                        if subcmd == "GETACK" && wildcard == "*" {
                            println!("[replication_loop] ✅ REPLCONF GETACK * → Responding with ACK 0");
                            write_array(&mut writer, &["REPLCONF", "ACK", "0"])?;
                            writer.flush()?;
                            println!("[replication_loop] 🚀 Sent: [REPLCONF ACK 0]");
                            continue;
                        }
                    } else {
                        println!("[replication_loop] ⚠️ Invalid REPLCONF arg count: {}", args.len());
                    }
                }

                // Normal write command replay
                println!("[replication_loop] 🔁 Replaying write-type command if applicable…");
                replay_cmd(&cmd, &mut writer, &args, &mut ctx)?;
                println!("[replication_loop] ✅ Finished replay.");
            }
        }
    }

    println!("[replication_loop] ✅ Clean shutdown.");
    Ok(())
}
