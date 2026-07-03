use raft_kv::net::{WireMessage, read_frame, write_frame};
use raft_kv::{ClientRequest, Command};
use std::env;
use std::io;
use std::net::TcpStream;
use std::process::ExitCode;

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
    let args: Vec<String> = env::args().collect();
    if args.len() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: raft-client <addr> set <key> <value> | raft-client <addr> get <key>",
        ));
    }
    let command = match args[2].as_str() {
        "set" if args.len() == 5 => Command::Set {
            key: args[3].clone(),
            value: args[4].clone(),
        },
        "get" if args.len() == 4 => Command::Get {
            key: args[3].clone(),
        },
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: raft-client <addr> set <key> <value> | raft-client <addr> get <key>",
            ));
        }
    };
    let request = ClientRequest { command };
    let mut stream = TcpStream::connect(&args[1])?;
    write_frame(&mut stream, &WireMessage::Client(request))?;
    match read_frame(&mut stream)? {
        WireMessage::ClientReply(reply) => {
            println!(
                "success={} leader={:?} response={:?}",
                reply.success, reply.leader_id, reply.response
            );
            Ok(())
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "expected client reply",
        )),
    }
}
