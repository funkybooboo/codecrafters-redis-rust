mod config;
mod resp;
mod rdb;
mod commands;
mod server;

use std::{collections::HashMap, net::TcpListener, sync::{Arc, Mutex}, thread, time::Instant};

use crate::config::parse_config;
use crate::rdb::load_rdb_snapshot;
use crate::server::handle_client;

fn main() -> std::io::Result<()> {
    // 1) Read CLI flags
    let cfg = parse_config();

    // 2) Load initial RDB snapshot (one key only)
    let snapshot = load_rdb_snapshot(format!("{}/{}", cfg.dir, cfg.dbfilename))?;
    let store_map: HashMap<_, _> = snapshot
        .into_iter()
        .map(|(k,v)| (k, (v, None::<Instant>)))
        .collect();

    let store = Arc::new(Mutex::new(store_map));
    let cfg   = Arc::new(cfg);

    // 3) Start listening
    let listener = TcpListener::bind("127.0.0.1:6379")?;
    println!("Listening on 127.0.0.1:6379…");

    for stream in listener.incoming() {
        let stream = stream?;
        let s = Arc::clone(&store);
        let c = Arc::clone(&cfg);
        thread::spawn(move || {
            if let Err(e) = handle_client(stream, s, c) {
                eprintln!("Client error: {}", e);
            }
        });
    }

    Ok(())
}
