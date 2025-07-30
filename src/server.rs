use std::{
    io::{self, Write},
    net::TcpStream,
};

use crate::resp::{read_array, write_array, write_error};
use crate::commands::{is_write_cmd, make_registry};
use crate::Context;
use crate::role::Role;

/// Now takes ownership of a cloneable Context
pub fn handle_client(
    stream: TcpStream,
    ctx: Context,
) -> io::Result<()> {
    let mut reader = io::BufReader::new(stream.try_clone()?);
    let mut writer = stream;

    let registry = make_registry();

    while let Some(args) = read_array(&mut reader)? {
        if args.is_empty() { continue; }
        let cmd = args[0].to_uppercase();

        if let Some(f) = registry.get(&cmd) {
            f(&mut writer, &args, &ctx)?;
        } else {
            write_error(&mut writer, "unknown command")?;
        }
        writer.flush()?;

        // propagate writes if master
        if ctx.cfg.role == Role::Master && is_write_cmd(&cmd) {
            let items: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
            let mut reps = ctx.replicas.lock().unwrap();
            reps.retain_mut(|rep| {
                if let Err(e) = write_array(rep, &items) {
                    eprintln!("Replication to {:?} failed: {}", rep.peer_addr(), e);
                    false
                } else {
                    true
                }
            });
        }
    }

    Ok(())
}
