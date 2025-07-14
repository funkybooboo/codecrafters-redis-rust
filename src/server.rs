use std::{
    io::{self, Write},
    net::TcpStream,
    sync::Arc,
};
use std::sync::Mutex;
use crate::config::ServerConfig;
use crate::resp::{read_array, write_array, write_error};
use crate::commands::{is_write_cmd, make_registry, Context};
use crate::rdb::Store;
use crate::role::Role;

pub fn handle_client(
    stream: TcpStream,
    store: Arc<Store>,
    cfg: Arc<ServerConfig>,
    replicas: Arc<Mutex<Vec<TcpStream>>>,
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
        let ctx = Context { cfg: &cfg, store: &store, replicas: Arc::clone(&replicas) };

        if let Some(cmd_fn) = registry.get(&cmd_name) {
            // Call the matched command
            cmd_fn(&mut writer, &args, &ctx)?;
        } else {
            // Unknown command
            write_error(&mut writer, "unknown command")?;
        }

        writer.flush()?;
        
        // --- propagate write‐type commands to every replica ---
        if cfg.role == Role::Master && is_write_cmd(&cmd_name) {
            // build the original RESP array
            let items: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        
            let mut reps = replicas.lock().unwrap();
            // retain only healthy connections, drop any that error out
            reps.retain_mut(|rep| {
                if let Err(e) = write_array(rep, &items) {
                    eprintln!("Replication to {:?} failed: {}", rep.peer_addr(), e);
                    false
                } else {
                    true
                }
            });
        }
    }

    Ok(())
}
