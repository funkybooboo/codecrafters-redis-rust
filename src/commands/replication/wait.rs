use crate::context::Context;
use crate::resp::{encode_int, encode_resp_error, write_resp_array};
use std::io;
use std::io::Write;
use std::thread::sleep;
use std::time::{Duration, Instant};

pub fn cmd_wait(args: &[String], ctx: &mut Context) -> io::Result<Vec<u8>> {
    // exactly two arguments
    if args.len() != 3 {
        return Ok(encode_resp_error("usage: WAIT <num_replicas> <timeout_ms>"));
    }

    let needed = match args[1].parse::<usize>() {
        Ok(n) => n,
        Err(_) => return Ok(encode_resp_error("ERR invalid replica count")),
    };
    let timeout_ms = match args[2].parse::<u64>() {
        Ok(ms) => ms,
        Err(_) => return Ok(encode_resp_error("ERR invalid timeout")),
    };

    // snapshot the offset our replicas must reach
    let target = ctx.master_repl_offset;
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);

    println!(
        "[cmd_wait] WAIT for {} replicas to ACK offset {} (timeout={}ms)",
        needed, target, timeout_ms
    );

    // send exactly one REPLCONF GETACK * to each replica
    {
        let mut reps = ctx.replicas.lock().unwrap();
        for (&addr, (rs, _)) in reps.iter_mut() {
            let getack = ["REPLCONF", "GETACK", "*"];
            write_resp_array(rs, &getack)?;
            rs.flush()?;
            println!("[cmd_wait] Sent GETACK to replica {}", addr);
        }
    }

    // spin until enough ACKs or we time out
    let mut acked = 0;
    while Instant::now() < deadline {
        acked = {
            let reps = ctx.replicas.lock().unwrap();
            reps
                .values()
                .filter(|&(_, last_ack)| *last_ack >= target)
                .count()
        };
        if acked >= needed {
            println!("[cmd_wait] Required ACKs received: {}", acked);
            break;
        }
        sleep(Duration::from_millis(1));
    }

    if acked < needed {
        println!("[cmd_wait] Timeout reached with {} ACKs", acked);
    }
    Ok(encode_int(acked as i64))
}
