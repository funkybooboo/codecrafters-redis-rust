mod config;
mod resp;
mod rdb;
mod commands;
mod server;
mod role;
mod handshakes;
mod utils;

use std::{io, net::TcpListener, sync::{Arc, Mutex}};
use std::collections::HashMap;
use std::net::TcpStream;
use crate::config::parse_config;
use crate::handshakes::replica_handshake;
use crate::rdb::load_rdb_snapshot;
use crate::role::Role;
use crate::server::{handle_client, replication_loop, BlockingList};

fn main() -> io::Result<()> {
    // 1) CLI flags
    let cfg = parse_config();

    let replicas: Arc<Mutex<Vec<TcpStream>>> = Arc::new(Mutex::new(Vec::new()));

    let blocking_clients: BlockingList = Arc::new(Mutex::new(HashMap::new()));

    // If we're a replica, connect & handshake *and* keep that socket
    let maybe_replication_stream = if cfg.role == Role::Slave {
        Some(replica_handshake(&cfg)?)
    } else {
        None
    };

    // 2) Load every (key, (value, expiry?)) from RDB
    let snapshot = load_rdb_snapshot(format!("{}/{}", cfg.dir, cfg.dbfilename))?;
    let store = Arc::new(Mutex::new(snapshot));
    let cfg = Arc::new(cfg);

    // 2b) If replica, spawn the propagation‐processor
    if let Some(replica_stream) = maybe_replication_stream {
        let store_clone = Arc::clone(&store);
        let cfg_clone   = Arc::clone(&cfg);
        std::thread::spawn(move || {
            if let Err(e) = replication_loop(replica_stream, store_clone, cfg_clone) {
                eprintln!("replication error: {}", e);
            }
        });
    }

    // 3) Serve
    let listener = TcpListener::bind(format!("127.0.0.1:{}", cfg.port))?;
    println!("Listening on 127.0.0.1:{}...", cfg.port);

    for stream in listener.incoming() {
        let stream = stream?;
        let s       = Arc::clone(&store);
        let c       = Arc::clone(&cfg);
        let reps    = Arc::clone(&replicas);
        let bc = Arc::clone(&blocking_clients);
        std::thread::spawn(move || {
            if let Err(e) = handle_client(stream, s, c, reps, bc) {
                eprintln!("Client error: {}", e);
            }
        });
    }

    Ok(())
}
