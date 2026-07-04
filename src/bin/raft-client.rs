use raft_kv::net::{WireMessage, read_frame, write_frame};
use raft_kv::{ClientReply, ClientRequest};
use std::env;
use std::io;
use std::net::TcpStream;
use std::process::ExitCode;
use std::time::Duration;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("raft-client: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> io::Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.len() < 3 {
        eprintln!("usage: raft-client <peer>... set <key> <value>");
        eprintln!("       raft-client <peer>... get <key>");
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "not enough args",
        ));
    }

    let cmd_pos = args
        .iter()
        .position(|a| a == "set" || a == "get")
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "command must be 'set' or 'get'",
            )
        })?;
    let peers: Vec<String> = args[..cmd_pos].to_vec();
    let cmd_args = &args[cmd_pos..];

    let request = match cmd_args[0].as_str() {
        "set" if cmd_args.len() == 3 => ClientRequest::Set {
            key: cmd_args[1].clone(),
            value: cmd_args[2].clone(),
        },
        "get" if cmd_args.len() == 2 => ClientRequest::Get {
            key: cmd_args[1].clone(),
        },
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: raft-client <peer>... set <key> <value> | raft-client <peer>... get <key>",
            ));
        }
    };

    let result = send_with_redirect(&peers, &request)?;
    println!(
        "success={} leader={:?} response={:?}",
        result.success, result.leader_id, result.response
    );
    Ok(())
}

fn send_with_redirect(peers: &[String], request: &ClientRequest) -> io::Result<ClientReply> {
    const MAX_ATTEMPTS: usize = 10;

    for _ in 0..MAX_ATTEMPTS {
        for addr in peers {
            match send_one(addr, request) {
                Ok(reply) if reply.success => return Ok(reply),
                Err(_) => {
                    std::thread::sleep(Duration::from_millis(20));
                }
                _ => {}
            }
        }
    }

    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        format!("no leader found after {MAX_ATTEMPTS} attempts"),
    ))
}

fn send_one(addr: &str, request: &ClientRequest) -> io::Result<ClientReply> {
    let mut stream = TcpStream::connect(addr)?;
    stream.set_read_timeout(Some(Duration::from_secs(1)))?;
    stream.set_write_timeout(Some(Duration::from_secs(1)))?;
    write_frame(&mut stream, &WireMessage::ClientRequest(request.clone()))?;
    match read_frame(&mut stream)? {
        WireMessage::ClientReply(reply) => Ok(reply),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "expected client reply",
        )),
    }
}
