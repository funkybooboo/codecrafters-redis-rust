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
        let cmd = args[0].to_uppercase();

        // Only queue *other* commands during a transaction:
        if ctx.in_transaction && cmd != "MULTI" && cmd != "EXEC" && cmd != "DISCARD" {
            ctx.queued.push((cmd.clone(), args.to_vec()));
            write_simple_string(&mut writer, "QUEUED")?;
            writer.flush()?;
            continue;
        }

        // Otherwise dispatch MULTI, EXEC, DISCARD, or any normal cmd
        dispatch_cmd(&cmd, &mut writer, &args, &mut ctx)?;
        writer.flush()?;

        // Propagate real writes (outside of EXEC‐DISPATCHed block)
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
