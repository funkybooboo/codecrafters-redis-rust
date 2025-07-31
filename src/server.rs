use crate::commands::{dispatch_cmd, is_write_cmd};
use crate::resp::{read_array, write_array, write_simple_string};
use crate::role::Role;
use crate::Context;
use std::{
    io::{self, Write},
    net::TcpStream,
};

pub fn handle_client(stream: TcpStream, mut ctx: Context) -> io::Result<()> {
    let mut reader = io::BufReader::new(stream.try_clone()?);
    let mut writer = stream;

    while let Some(args) = read_array(&mut reader)? {
        if args.is_empty() {
            continue;
        }

        // Normalize command name once
        let cmd = args[0].to_uppercase();

        // If we're inside a MULTI/EXEC block *and* it's not MULTI or EXEC itself,
        // queue it and reply "+QUEUED" without executing any logic.
        if ctx.in_transaction && cmd != "MULTI" && cmd != "EXEC" {
            ctx.queued.push((cmd.clone(), args.to_vec()));
            write_simple_string(&mut writer, "QUEUED")?;
            writer.flush()?;
            continue;
        }

        // Otherwise, dispatch MULTI, EXEC, or any normal command
        dispatch_cmd(&cmd, &mut writer, &args, &mut ctx)?;
        writer.flush()?;

        // Propagate writes (only outside an open transaction)
        if !ctx.in_transaction
            && ctx.cfg.role == Role::Master
            && is_write_cmd(&cmd)
        {
            let items: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
            let mut reps = ctx.replicas.lock().unwrap();
            reps.retain_mut(|rep| write_array(rep, &items).is_ok());
        }
    }

    Ok(())
}
