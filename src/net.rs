use crate::{ClientReply, ClientRequest, Message};
use serde::{Deserialize, Serialize};
use std::io::{self, Read, Write};
use std::net::TcpStream;

const MAX_FRAME_LEN: usize = 16 * 1024 * 1024;

/// Wire-level dispatch: all messages on the TCP socket use this enum.
/// Servers receive Peer and ClientRequest; they send Peer and ClientReply.
/// Receiving ClientReply on the server is a protocol error.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum WireMessage {
    Peer(Message),
    ClientRequest(ClientRequest),
    ClientReply(ClientReply),
}

pub fn write_frame(stream: &mut TcpStream, message: &WireMessage) -> io::Result<()> {
    write_bincode(stream, message)
}

pub fn read_frame(stream: &mut TcpStream) -> io::Result<WireMessage> {
    read_bincode(stream)
}

pub fn write_peer_frame(stream: &mut TcpStream, message: Message) -> io::Result<()> {
    write_frame(stream, &WireMessage::Peer(message))
}

pub fn write_reply_frame(stream: &mut TcpStream, reply: ClientReply) -> io::Result<()> {
    write_frame(stream, &WireMessage::ClientReply(reply))
}

fn write_bincode<T: Serialize>(stream: &mut TcpStream, value: &T) -> io::Result<()> {
    let bytes = bincode::serialize(value).map_err(io::Error::other)?;
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

fn read_bincode<T: for<'de> Deserialize<'de>>(stream: &mut TcpStream) -> io::Result<T> {
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
    use std::net::TcpListener;
    use std::thread;

    #[test]
    fn client_frame_round_trips() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            read_frame(&mut stream).unwrap()
        });
        let mut stream = TcpStream::connect(addr).unwrap();
        let message = WireMessage::ClientRequest(ClientRequest::Set {
            key: "foo".to_string(),
            value: "bar".to_string(),
        });
        write_frame(&mut stream, &message).unwrap();
        assert_eq!(handle.join().unwrap(), message);
    }

    #[test]
    fn peer_frame_round_trips() {
        use crate::{AppendEntries, Rpc};
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            read_frame(&mut stream).unwrap()
        });
        let mut stream = TcpStream::connect(addr).unwrap();
        let message = WireMessage::Peer(Message {
            from: 0,
            to: 1,
            rpc: Rpc::AppendEntries(AppendEntries {
                term: 1,
                leader_id: 0,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            }),
        });
        write_frame(&mut stream, &message).unwrap();
        let received = handle.join().unwrap();
        match &received {
            WireMessage::Peer(msg) => assert_eq!(msg.from, 0),
            _ => panic!("expected Peer message"),
        }
    }
}
