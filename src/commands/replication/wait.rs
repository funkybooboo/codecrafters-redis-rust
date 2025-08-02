use crate::resp::encode_int;
use std::io;
use crate::context::Context;

pub fn cmd_wait(_args: &[String], ctx: &mut Context) -> io::Result<Vec<u8>> {
    let count = {
        let replicas = ctx.replicas.lock().unwrap();
        replicas.len()
    };
    println!("[WAIT] Responding with connected replica count: {}", count);
    Ok(encode_int(count as i64))
}
