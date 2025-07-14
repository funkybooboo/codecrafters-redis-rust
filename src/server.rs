use std::{
    io::{self, Write},
    net::TcpStream,
    sync::Arc,
};

use crate::config::ServerConfig;
use crate::resp::{read_array, write_error};
use crate::commands::{make_registry, Context};
use crate::rdb::Store;

pub fn handle_client(
    stream: TcpStream,
    store: Arc<Store>,
    cfg: Arc<ServerConfig>,
) -> io::Result<()> {
    let mut reader = io::BufReader::new(stream.try_clone()?);
    let mut writer = stream;

    // Build the map of command names → functions
    let registry = make_registry();

    while let Some(args) = read_array(&mut reader)? {
        if args.is_empty() {
            continue;
        }
        let cmd_name = args[0].to_uppercase();
        let ctx = Context { cfg: &cfg, store: &store };

        if let Some(cmd_fn) = registry.get(&cmd_name) {
            // Call the matched command
            cmd_fn(&mut writer, &args, &ctx)?;
        } else {
            // Unknown command
            write_error(&mut writer, "unknown command")?;
        }

        writer.flush()?;
    }

    Ok(())
}
