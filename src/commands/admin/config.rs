use crate::commands::Context;
use crate::resp::{check_len, write_bulk_string, write_error};
use std::io;
use std::io::Write;
use std::net::TcpStream;

/// CONFIG GET <dir|dbfilename>
pub fn cmd_config(out: &mut TcpStream, args: &[String], ctx: &Context) -> io::Result<()> {
    if !check_len(out, args, 3, "usage: CONFIG GET <dir|dbfilename>") {
        return Ok(());
    }

    let key = &args[2];
    let val = match key.as_str() {
        "dir" => &ctx.cfg.dir,
        "dbfilename" => &ctx.cfg.dbfilename,
        _ => {
            write_error(out, "unknown config parameter")?;
            return Ok(());
        }
    };

    // array of two bulk-strings
    out.write_all("*2\r\n".to_string().as_bytes())?;
    write_bulk_string(out, key)?;
    write_bulk_string(out, val)
}
