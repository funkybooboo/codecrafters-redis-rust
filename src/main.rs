mod commands;
mod config;
mod rdb;
mod replication;
mod resp;
mod role;
mod server;

use crate::{
    config::{parse_config, ServerConfig},
    rdb::{load_rdb_snapshot, Store},
    replication::replication_loop,
    role::Role,
    server::handle_client,
};
use std::collections::HashMap;
use std::net::TcpStream;
use std::{
    io,
    net::TcpListener,
    sync::{Arc, Mutex},
};

pub type BlockingList = Arc<Mutex<HashMap<String, Vec<TcpStream>>>>;

/// Holds *both* the global server state (all Arcs)
/// and the per‐connection transaction state (plain fields).
#[derive(Clone)]
pub struct Context {
    // global:
    pub cfg:       Arc<ServerConfig>,
    pub store:     Arc<Store>,
    pub replicas:  Arc<Mutex<Vec<TcpStream>>>,
    pub blocking:  BlockingList,

    // per‐connection:
    pub in_transaction: bool,                       // tracks MULTI…EXEC
    pub queued: Vec<(String, Vec<String>)>,         // buffers (cmd, args)
}

fn main() -> io::Result<()> {
    let cfg: ServerConfig = parse_config();
    let cfg = Arc::new(cfg);

    let raw_snapshot = load_rdb_snapshot(format!("{}/{}", cfg.dir, cfg.dbfilename))?;
    let store = Arc::new(Mutex::new(raw_snapshot)); // Store = Arc<Mutex<…>>

    let replicas = Arc::new(Mutex::new(Vec::new()));
    let blocking_clients: BlockingList = Arc::new(Mutex::new(HashMap::new()));

    // build a single Context
    let base_ctx = Context {
        cfg:              cfg.clone(),
        store:            store.clone(),
        replicas:         replicas.clone(),
        blocking:         blocking_clients.clone(),
        in_transaction:   false,
        queued:           Vec::new(),
    };

    // If this node is a slave, spin up replication_loop
    if cfg.role == Role::Slave {
        let ctx_clone = base_ctx.clone();
        let replica_stream = replication::replica_handshake(&cfg)?;
        std::thread::spawn(move || {
            if let Err(e) = replication_loop(replica_stream, ctx_clone) {
                eprintln!("replication error: {e}");
            }
        });
    }

    // … now listen for clients …
    let listener = TcpListener::bind(format!("127.0.0.1:{}", cfg.port))?;
    println!("Listening on 127.0.0.1:{}…", cfg.port);

    for stream in listener.incoming() {
        let stream = stream?;
        let ctx_clone = base_ctx.clone();
        std::thread::spawn(move || {
            if let Err(e) = handle_client(stream, ctx_clone) {
                eprintln!("Client error: {e}");
            }
        });
    }

    Ok(())
}
