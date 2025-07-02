use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::io::Result;

fn handle_client(mut stream: TcpStream) -> Result<()> {
    let mut buf = [0u8; 512];

    loop {
        let n = stream.read(&mut buf)?;
        if n == 0 {
            break; // client closed
        }
        stream.write_all(b"+PONG\r\n")?;
    }
    Ok(())
}

fn main() -> Result<()> {
    let ip = "127.0.0.1";
    let port = "6379";
    let address = format!("{ip}:{port}");
    let listener = TcpListener::bind(&address)?;
    println!("Listening on {address}...");

    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                if let Err(e) = handle_client(stream) {
                    eprintln!("client error: {}", e);
                }
            }
            Err(e) => eprintln!("accept error: {}", e),
        }
    }
    Ok(())
}
