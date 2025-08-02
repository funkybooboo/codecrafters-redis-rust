extern crate core;

mod commands;
mod config;
mod rdb;
mod replication;
mod resp;
mod role;
mod server;
mod context;

use crate::{
    config::{parse_config, ServerConfig},
    rdb::load_rdb_snapshot,
    replication::replication_loop,
    role::Role,
    server::handle_client,
};
use std::collections::HashMap;
use std::{
    io,
    net::TcpListener,
    sync::{Arc, Mutex},
};
use crate::context::{BlockingList, Context};

fn main() -> io::Result<()> {
    println!("[main] Starting Redis-like server...");

    let cfg: ServerConfig = parse_config();
    let cfg = Arc::new(cfg);
    println!("[main] Configuration parsed: {:?}", cfg);

    let snapshot_path = format!("{}/{}", cfg.dir, cfg.dbfilename);
    println!("[main] Loading RDB snapshot from {}", snapshot_path);
    let raw_snapshot = load_rdb_snapshot(snapshot_path)?;
    println!("[main] Snapshot loaded successfully.");

    let store = Arc::new(Mutex::new(raw_snapshot));
    let replicas = Arc::new(Mutex::new(Vec::new()));
    let blocking_clients: BlockingList = Arc::new(Mutex::new(HashMap::new()));

    // build a single Context
    let base_ctx = Context {
        cfg: cfg.clone(),
        store: store.clone(),
        replicas: replicas.clone(),
        blocking: blocking_clients.clone(),
        master_repl_offset: 0,
        in_transaction: false,
        queued: Vec::new(),
        this_client: None,
    };

    println!("[main] Context initialized.");

    // If this node is a slave, spin up replication_loop
    if cfg.role == Role::Slave {
        println!("[main] Node is a replica. Starting replication handshake...");
        let ctx_clone = base_ctx.clone();
        let replica_stream = replication::replica_handshake(&cfg)?;
        println!("[main] Replication handshake successful. Spawning replication loop.");
        std::thread::spawn(move || {
            if let Err(e) = replication_loop(replica_stream, ctx_clone) {
                eprintln!("[replication_loop] replication error: {e}");
            }
        });
    }

    let bind_addr = format!("127.0.0.1:{}", cfg.port);
    let listener = TcpListener::bind(&bind_addr)?;
    println!("[main] Listening on {}...", bind_addr);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("[main] Accepted new client connection from {:?}", stream.peer_addr());
                let ctx_clone = base_ctx.clone();
                std::thread::spawn(move || {
                    if let Err(e) = handle_client(stream, ctx_clone) {
                        eprintln!("[handle_client] Client error: {e}");
                    }
                });
            }
            Err(e) => eprintln!("[main] Connection failed: {e}"),
        }
    }

    Ok(())
}
