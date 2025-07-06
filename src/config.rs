use std::env;

/// Dir + filename for RDB persistence.
#[derive(Debug)]
pub struct ServerConfig {
    pub dir: String,
    pub dbfilename: String,
}

/// Read `--dir <path>` and `--dbfilename <name>` from CLI.
pub fn parse_config() -> ServerConfig {
    let mut dir       = ".".to_string();
    let mut dbfilename = "dump.rdb".to_string();
    let args: Vec<_> = env::args().collect();
    let mut i = 1;
    while i + 1 < args.len() {
        match args[i].as_str() {
            "--dir"        => dir        = args[i + 1].clone(),
            "--dbfilename" => dbfilename = args[i + 1].clone(),
            _ => {}
        }
        i += 2;
    }
    ServerConfig { dir, dbfilename }
}
