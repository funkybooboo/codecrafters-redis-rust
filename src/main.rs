extern crate core;

mod commands;
mod config;
mod context;
mod rdb;
mod replication;
mod resp;
mod role;
mod server;

use crate::{
    config::{parse_config, ServerConfig},
    context::{BlockingList, Context},
    rdb::load_rdb_snapshot_from_path,
    replication::connect_and_sync_master,
    role::Role,
    server::serve_client_connection,
};

use std::{
    collections::HashMap,
    io,
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
};
use std::collections::HashSet;
use crate::context::{Replicas};
use crate::rdb::Store;

fn main() -> io::Result<()> {
    println!("[main] Starting Redis-like server...");

    let cfg = Arc::new(parse_config());
    println!("[main] Configuration parsed: {:?}", cfg);

    let shared_ctx = build_context(&cfg)?;
    println!("[main] Context initialized.");

    if cfg.role == Role::Slave {
        spawn_replica_sync_thread(shared_ctx.clone());
    }

    start_tcp_server(cfg.port, shared_ctx)?;

    println!("[main] Shutting down server cleanly.");
    Ok(())
}

fn build_context(cfg: &Arc<ServerConfig>) -> io::Result<Context> {
    let store_data = match cfg.role {
        Role::Master => {
            let snapshot_path = format!("{}/{}", cfg.dir, cfg.dbfilename);
            println!("[init] Loading RDB snapshot from {}", snapshot_path);
            let snapshot = load_rdb_snapshot_from_path(snapshot_path)?;
            println!("[init] Snapshot loaded successfully.");
            snapshot
        }
        Role::Slave => {
            println!("[init] Replica node - skipping local snapshot load.");
            HashMap::new()
        }
    };

    let store: Arc<Store> = Arc::new(Mutex::new(store_data));
    let replicas: Replicas = Arc::new(Mutex::new(HashMap::<SocketAddr, (TcpStream, usize)>::new()));
    let blocking_clients: BlockingList = Arc::new(Mutex::new(HashMap::new()));

    Ok(Context {
        cfg: cfg.clone(),
        store,
        replicas,
        blocking: blocking_clients,
        master_repl_offset: 0,
        pending_writes: Arc::new(Mutex::new(Vec::new())),
        in_transaction: false,
        queued: Vec::new(),
        this_client: None,
        pubsub: Arc::new(Mutex::new(HashMap::new())),
        subscribed_channels: HashSet::new(),
    })
}

fn spawn_replica_sync_thread(ctx: Context) {
    println!("[main] Node is a replica. Spawning replication thread...");
    let ctx_clone = ctx.clone();
    thread::spawn(move || {
        println!("[replication_thread] Starting replication handler...");
        if let Err(e) = connect_and_sync_master(ctx_clone) {
            eprintln!("[replication_thread] Replication error: {}", e);
        }
        println!("[replication_thread] Replication thread exited.");
    });
}

fn start_tcp_server(port: u16, ctx: Context) -> io::Result<()> {
    let bind_addr = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(&bind_addr)?;
    println!("[net] Listening for clients on {}...", bind_addr);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let peer = stream
                    .peer_addr()
                    .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)));
                println!("[net] Accepted client from {:?}", peer);

                let ctx_clone = ctx.clone();
                thread::spawn(move || {
                    if let Err(e) = serve_client_connection(stream, ctx_clone) {
                        eprintln!("[client] Error: {}", e);
                    }
                });
            }
            Err(e) => {
                eprintln!("[net] Failed to accept connection: {}", e);
            }
        }
    }

    Ok(())
}
