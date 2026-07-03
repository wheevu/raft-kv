use crate::{ClientReply, ClientRequest, Message};
use serde::{Deserialize, Serialize};
use std::io::{self, Read, Write};
use std::net::TcpStream;

const MAX_FRAME_LEN: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum WireMessage {
    Peer(Message),
    Client(ClientRequest),
    ClientReply(ClientReply),
}

pub fn write_frame(stream: &mut TcpStream, message: &WireMessage) -> io::Result<()> {
    let bytes = bincode::serialize(message).map_err(io::Error::other)?;
    if bytes.len() > MAX_FRAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "frame too large",
        ));
    }
    let len = u32::try_from(bytes.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "frame too large"))?;
    stream.write_all(&len.to_be_bytes())?;
    stream.write_all(&bytes)?;
    stream.flush()
}

pub fn read_frame(stream: &mut TcpStream) -> io::Result<WireMessage> {
    let mut len = [0; 4];
    stream.read_exact(&mut len)?;
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_FRAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame too large",
        ));
    }
    let mut bytes = vec![0; len];
    stream.read_exact(&mut bytes)?;
    bincode::deserialize(&bytes).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Command, Node};
    use std::net::TcpListener;
    use std::thread;

    #[test]
    fn length_prefixed_frame_round_trips() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            read_frame(&mut stream).unwrap()
        });
        let mut stream = TcpStream::connect(addr).unwrap();
        let message = WireMessage::Client(ClientRequest {
            command: Command::Set {
                key: "foo".to_string(),
                value: "bar".to_string(),
            },
        });
        write_frame(&mut stream, &message).unwrap();
        assert_eq!(handle.join().unwrap(), message);

        let _ = Node::new(0, vec![1]);
    }
}
