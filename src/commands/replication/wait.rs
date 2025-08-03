use crate::resp::encode_int;
use std::io;
use crate::context::Context;

pub fn cmd_wait(_args: &[String], ctx: &mut Context) -> io::Result<Vec<u8>> {
    let count = match ctx.replicas.lock() {
        Ok(replicas) => {
            let count = replicas.len();
            println!("[cmd_wait] Connected replica count: {}", count);
            count
        }
        Err(e) => {
            eprintln!("[cmd_wait] Failed to acquire lock on replicas: {}", e);
            0
        }
    };

    Ok(encode_int(count as i64))
}
