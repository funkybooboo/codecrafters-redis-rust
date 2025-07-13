mod config;
mod resp;
mod rdb;
mod commands;
mod server;

use std::{net::TcpListener, sync::{Arc, Mutex}};

use crate::config::parse_config;
use crate::rdb::load_rdb_snapshot;
use crate::server::handle_client;

fn main() -> std::io::Result<()> {
    // 1) CLI flags
    let cfg = parse_config();

    // 2) Load every (key, (value, expiry?)) from RDB
    let snapshot = load_rdb_snapshot(format!("{}/{}", cfg.dir, cfg.dbfilename))?;
    let store = Arc::new(Mutex::new(snapshot));
    let cfg = Arc::new(cfg);

    // 3) Serve
    let listener = TcpListener::bind(format!("127.0.0.1:{}", cfg.port))?;
    println!("Listening on 127.0.0.1:{}...", cfg.port);

    for stream in listener.incoming() {
        let s = Arc::clone(&store);
        let c = Arc::clone(&cfg);
        let stream = stream?;
        std::thread::spawn(move || {
            if let Err(e) = handle_client(stream, s, c) {
                eprintln!("Client error: {}", e);
            }
        });
    }

    Ok(())
}
