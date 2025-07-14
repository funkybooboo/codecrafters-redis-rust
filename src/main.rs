mod config;
mod resp;
mod rdb;
mod commands;
mod server;
mod role;
mod handshakes;
mod utils;

use std::{io, net::TcpListener, sync::{Arc, Mutex}};
use std::net::TcpStream;
use crate::config::parse_config;
use crate::handshakes::replica_handshake;
use crate::rdb::load_rdb_snapshot;
use crate::role::Role;
use crate::server::handle_client;

fn main() -> io::Result<()> {
    // 1) CLI flags
    let cfg = parse_config();

    let replicas: Arc<Mutex<Vec<TcpStream>>> = Arc::new(Mutex::new(Vec::new()));

    // Part 1 of replica handshake: send PING to master
    if cfg.role == Role::Slave {
        replica_handshake(&cfg)?;
    }

    // 2) Load every (key, (value, expiry?)) from RDB
    let snapshot = load_rdb_snapshot(format!("{}/{}", cfg.dir, cfg.dbfilename))?;
    let store = Arc::new(Mutex::new(snapshot));
    let cfg = Arc::new(cfg);

    // 3) Serve
    let listener = TcpListener::bind(format!("127.0.0.1:{}", cfg.port))?;
    println!("Listening on 127.0.0.1:{}...", cfg.port);

    for stream in listener.incoming() {
        let stream = stream?;
        let s       = Arc::clone(&store);
        let c       = Arc::clone(&cfg);
        let reps    = Arc::clone(&replicas);
        std::thread::spawn(move || {
            if let Err(e) = handle_client(stream, s, c, reps) {
                eprintln!("Client error: {}", e);
            }
        });
    }

    Ok(())
}
