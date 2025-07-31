use crate::commands::replay_cmd;
use crate::config::ServerConfig;
use crate::resp::{read_array, write_array};
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
    let mut master = TcpStream::connect((&cfg.master_host[..], cfg.master_port))?;
    let mut reader = BufReader::new(master.try_clone()?);

    write_array(&mut master, &["PING"])?;
    let _ = wait_for_it(&mut reader)?;

    write_array(
        &mut master,
        &["REPLCONF", "listening-port", &cfg.port.to_string()],
    )?;
    let _ = wait_for_it(&mut reader)?;

    write_array(&mut master, &["REPLCONF", "capa", "psync2"])?;
    let _ = wait_for_it(&mut reader)?;

    write_array(&mut master, &["PSYNC", "?", "-1"])?;
    let _ = wait_for_it(&mut reader)?; // +FULLRESYNC <replid> <offset>

    let mut rdb_header = String::new();
    reader.read_line(&mut rdb_header)?;
    let rdb_header = rdb_header.trim_end();
    if !rdb_header.starts_with('$') {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Expected RDB length header"));
    }

    let rdb_len: usize = rdb_header[1..]
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid RDB length"))?;

    let mut rdb_buf = vec![0u8; rdb_len];
    reader.read_exact(&mut rdb_buf)?;

    Ok(master)
}

/// Reads exactly one line (`+\r\n`, `-ERR…\r\n`, etc.) from the master
fn wait_for_it(reader: &mut dyn BufRead) -> io::Result<String> {
    let mut line = String::new();
    reader.read_line(&mut line)?;
    Ok(line)
}

/// Read commands from master, replay only write‐type ones through your normal cmd_*
/// (which mutates ctx.store for you). Any “OK” they emit is ignored.
pub fn replication_loop(stream: TcpStream, mut ctx: Context) -> std::io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = stream;

    loop {
        let maybe_args = read_array(&mut reader);
        match maybe_args {
            Err(_) => break,
            Ok(None) => break,
            Ok(Some(args)) => {
                if args.is_empty() {
                    continue;
                }

                let cmd = args[0].to_uppercase();

                if cmd == "REPLCONF" {
                    if args.len() == 3 {
                        let subcmd = args[1].to_uppercase();
                        let wildcard = &args[2];
                        if subcmd == "GETACK" && wildcard == "*" {
                            write_array(&mut writer, &["REPLCONF", "ACK", "0"])?;
                            writer.flush()?;
                            continue;
                        }
                    }
                }

                replay_cmd(&cmd, &mut writer, &args, &mut ctx)?;
            }
        }
    }

    Ok(())
}
