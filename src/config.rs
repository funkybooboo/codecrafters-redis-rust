use crate::role::Role;
use std::env;

/// Dir + filename for RDB persistence.
#[derive(Debug)]
pub struct ServerConfig {
    pub dir: String,
    pub dbfilename: String,
    pub port: u16,
    pub role: Role,
    pub master_host: String,
    pub master_port: u16,
    pub master_replid: String,
    pub master_repl_offset: i32,
}

/// Read `--dir`, `--dbfilename`, `--port`, and `--replicaof` from CLI.
pub fn parse_config() -> ServerConfig {
    let mut dir = ".".to_string();
    let mut dbfilename = "dump.rdb".to_string();
    let mut port: u16 = 6379;
    let mut role: Role = Role::Master;
    let mut master_host = String::new();
    let mut master_port: u16 = 0;
    let master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string();
    let master_repl_offset = 0;

    let args: Vec<_> = env::args().collect();
    let mut i = 1;
    while i + 1 < args.len() {
        match args[i].as_str() {
            "--dir" => {
                dir = args[i + 1].clone();
            }
            "--dbfilename" => {
                dbfilename = args[i + 1].clone();
            }
            "--port" => {
                port = args[i + 1].parse().expect("port must be a valid number");
            }
            "--replicaof" => {
                let mut parts = args[i + 1].split_whitespace();
                master_host = parts.next().expect("missing master host").to_string();
                master_port = parts
                    .next()
                    .expect("missing master port")
                    .parse()
                    .expect("master port must be a number");
                role = Role::Slave;
            }
            _ => {}
        }
        i += 2;
    }

    ServerConfig {
        dir,
        dbfilename,
        port,
        role,
        master_host,
        master_port,
        master_replid,
        master_repl_offset,
    }
}
