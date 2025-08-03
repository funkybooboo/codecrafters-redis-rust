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

    // Load snapshot only if master
    let store_data = match cfg.role {
        Role::Master => {
            let snapshot_path = format!("{}/{}", cfg.dir, cfg.dbfilename);
            println!("[main] Loading RDB snapshot from {}", snapshot_path);
            let raw_snapshot = load_rdb_snapshot(snapshot_path)?;
            println!("[main] Snapshot loaded successfully.");
            raw_snapshot
        }
        Role::Slave => {
            println!("[main] Replica node — skipping local snapshot load.");
            HashMap::new()
        }
    };

    let store = Arc::new(Mutex::new(store_data));
    let replicas = Arc::new(Mutex::new(Vec::new()));
    let blocking_clients: BlockingList = Arc::new(Mutex::new(HashMap::new()));

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

    if cfg.role == Role::Slave {
        println!("[main] Node is a replica. Spawning replication thread...");
        let ctx_clone = base_ctx.clone();
        std::thread::spawn(move || {
            println!("[replication_thread] Starting replication handler...");
            if let Err(e) = replication::handle_replication(ctx_clone) {
                eprintln!("[replication_thread] Replication error: {}", e);
            }
            println!("[replication_thread] Replication thread exited.");
        });
    }

    let bind_addr = format!("127.0.0.1:{}", cfg.port);
    let listener = TcpListener::bind(&bind_addr)?;
    println!("[main] Listening for clients on {}...", bind_addr);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!(
                    "[main] Accepted new client connection from {:?}. Spawning handler thread...",
                    stream.peer_addr().unwrap_or_else(|_| "[unknown]".parse().unwrap())
                );
                let ctx_clone = base_ctx.clone();
                std::thread::spawn(move || {
                    if let Err(e) = handle_client(stream, ctx_clone) {
                        eprintln!("[handle_client] Client error: {}", e);
                    }
                });
            }
            Err(e) => {
                eprintln!("[main] Failed to accept connection on {}: {}", bind_addr, e);
            }
        }
    }

    println!("[main] Shutting down server cleanly.");
    Ok(())
}
