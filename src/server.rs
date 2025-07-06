use std::{
    io::{self, BufReader, Write},
    net::TcpStream,
    sync::{Arc, Mutex},
};

use crate::config::ServerConfig;
use crate::resp::read_resp_array;
use crate::commands::{
    cmd_ping, cmd_echo, cmd_set, cmd_get, cmd_config, cmd_keys,
};

type Store = Mutex<std::collections::HashMap<String, (String, Option<std::time::Instant>)>>;

/// Handle one client: read RESP arrays, dispatch to `commands`, flush.
pub fn handle_client(
    stream: TcpStream,
    store: Arc<Store>,
    cfg: Arc<ServerConfig>,
) -> io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = stream;

    while let Some(args) = read_resp_array(&mut reader)? {
        if args.is_empty() { continue }
        let cmd = args[0].to_uppercase();
        let res = match cmd.as_str() {
            "PING"   => cmd_ping(&mut writer),
            "ECHO"   => cmd_echo(&mut writer, &args),
            "SET"    => cmd_set(&mut writer, &args, &store),
            "GET"    => cmd_get(&mut writer, &args, &store),
            "CONFIG" => cmd_config(&mut writer, &args, &cfg),
            "KEYS"   => cmd_keys(&mut writer, &args, &store),
            _        => writer.write_all(b"-ERR unknown command\r\n"),
        };
        res?; writer.flush()?;
    }

    Ok(())
}
