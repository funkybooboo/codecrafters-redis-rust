use crate::commands::{dispatch_cmd, is_write_cmd};
use crate::resp::{read_array, write_array};
use crate::role::Role;
use crate::Context;
use std::io::Write;
use std::{
    io::{self},
    net::TcpStream,
};

pub fn handle_client(stream: TcpStream, mut ctx: Context) -> io::Result<()> {
    let mut reader = io::BufReader::new(stream.try_clone()?);
    let mut writer = stream;

    while let Some(args) = read_array(&mut reader)? {
        if args.is_empty() {
            continue;
        }
        let cmd = args[0].to_uppercase();

        // run *any* command
        dispatch_cmd(&cmd, &mut writer, &args, &mut ctx)?;
        writer.flush()?;

        // then propagate writes
        if ctx.cfg.role == Role::Master && is_write_cmd(&cmd) {
            let items: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
            let mut reps = ctx.replicas.lock().unwrap();
            reps.retain_mut(|rep| write_array(rep, &items).is_ok());
        }
    }
    Ok(())
}
