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
}

pub fn parse_config() -> ServerConfig {
    println!("[config::parse_config] Parsing server configuration...");

    let mut dir = ".".to_string();
    let mut dbfilename = "dump.rdb".to_string();
    let mut port: u16 = 6379;
    let mut role: Role = Role::Master;
    let mut master_host = String::new();
    let mut master_port: u16 = 0;
    let master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string();

    let args: Vec<_> = env::args().collect();
    println!("[config::parse_config] Command-line arguments: {:?}", args);

    let mut i = 1;
    while i + 1 < args.len() {
        match args[i].as_str() {
            "--dir" => {
                dir = args[i + 1].clone();
                println!("[config::parse_config] --dir set to '{}'", dir);
            }
            "--dbfilename" => {
                dbfilename = args[i + 1].clone();
                println!("[config::parse_config] --dbfilename set to '{}'", dbfilename);
            }
            "--port" => {
                port = args[i + 1]
                    .parse()
                    .expect("[config::parse_config] Error: port must be a valid number");
                println!("[config::parse_config] --port set to {}", port);
            }
            "--replicaof" => {
                let mut parts = args[i + 1].split_whitespace();
                master_host = parts
                    .next()
                    .expect("[config::parse_config] Error: missing master host")
                    .to_string();
                master_port = parts
                    .next()
                    .expect("[config::parse_config] Error: missing master port")
                    .parse()
                    .expect("[config::parse_config] Error: master port must be a number");
                role = Role::Slave;
                println!(
                    "[config::parse_config] --replicaof set: master_host='{}', master_port={}, role='{}'",
                    master_host, master_port, role
                );
            }
            unknown => {
                println!("[config::parse_config] Warning: Unknown argument '{}'", unknown);
            }
        }
        i += 2;
    }

    let config = ServerConfig {
        dir,
        dbfilename,
        port,
        role,
        master_host,
        master_port,
        master_replid,
    };

    println!("[config::parse_config] Final parsed config: {:?}", config);
    config
}
