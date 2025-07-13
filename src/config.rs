use std::env;

/// Dir + filename for RDB persistence.
#[derive(Debug)]
pub struct ServerConfig {
    pub dir: String,
    pub dbfilename: String,
    pub port: u16,
    pub role: String,
    pub master_host: String,
    pub master_port: u16,
}

/// Read `--dir <path>` and `--dbfilename <name>`, etc. from CLI.
pub fn parse_config() -> ServerConfig {
    let mut dir       = ".".to_string();
    let mut dbfilename = "dump.rdb".to_string();
    let mut port = 6379;
    let mut role = "master".to_string();
    let mut master_host = "".to_string();
    let mut master_port= 0;
    let args: Vec<_> = env::args().collect();
    let mut i = 1;
    while i + 1 < args.len() {
        match args[i].as_str() {
            "--dir"        => dir        = args[i + 1].clone(),
            "--dbfilename" => dbfilename = args[i + 1].clone(),
            "--port" => port = args[i + 1]
                .clone()
                .parse::<u16>()
                .expect("port must be a number"),
            "--replicaof" => {
                let replicaof = &args[i + 1];
                let mut replicaof = replicaof.split_whitespace();
                master_host = replicaof
                    .next()
                    .expect("missing host")
                    .to_string();
                master_port = replicaof
                    .next()
                    .expect("missing port")
                    .parse::<u16>()
                    .expect("port must be a number");
                role = "slave".to_string();
            },
            _ => {}
        }
        i += 2;
    }
    ServerConfig { dir, dbfilename, port, role, master_host, master_port }
}
