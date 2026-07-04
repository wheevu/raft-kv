use raft_kv::net::{WireMessage, read_frame, write_peer_frame, write_reply_frame};
use raft_kv::storage::DurableState;
use raft_kv::storage::{load_node, save_node};
use raft_kv::{Node, NodeId};
use std::collections::HashMap;
use std::env;
use std::io;
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("raft-node: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> io::Result<()> {
    let config = Config::from_args(env::args().collect())?;
    let ids: Vec<_> = config.peers.keys().copied().collect();
    let peer_ids = ids.into_iter().filter(|&id| id != config.id).collect();
    let node = load_node(&config.state_path, config.id, peer_ids)?;
    let last_saved = DurableState::from_node(&node);
    let shared = Arc::new(Mutex::new(Runtime {
        node,
        started: Instant::now(),
        last_saved,
    }));
    let ticker = Arc::clone(&shared);
    let tick_config = config.clone();
    thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_millis(10));
            let messages = {
                let mut runtime = ticker.lock().expect("node mutex poisoned");
                let now = runtime.started.elapsed().as_millis() as u64;
                let messages = runtime.node.tick(now);
                if let Err(err) = runtime.persist_if_changed(&tick_config.state_path) {
                    eprintln!("persist tick: {err}");
                }
                messages
            };
            send_all(&tick_config.peers, messages);
        }
    });

    let bind = config
        .peers
        .get(&config.id)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing self address"))?;
    let listener = TcpListener::bind(bind)?;
    for stream in listener.incoming() {
        let stream = stream?;
        let shared = Arc::clone(&shared);
        let request_config = config.clone();
        thread::spawn(move || {
            if let Err(err) = handle_connection(stream, shared, request_config) {
                eprintln!("connection: {err}");
            }
        });
    }
    Ok(())
}

fn handle_connection(
    mut stream: TcpStream,
    shared: Arc<Mutex<Runtime>>,
    config: Config,
) -> io::Result<()> {
    match read_frame(&mut stream)? {
        WireMessage::Peer(message) => {
            let replies = {
                let mut runtime = shared.lock().expect("node mutex poisoned");
                let now = runtime.started.elapsed().as_millis() as u64;
                let replies = runtime.node.handle_message(message.from, message.rpc, now);
                if let Err(err) = runtime.persist_if_changed(&config.state_path) {
                    eprintln!("persist peer message: {err}");
                }
                replies
            };
            send_all(&config.peers, replies);
        }
        WireMessage::ClientRequest(request) => {
            let (reply, messages) = {
                let mut runtime = shared.lock().expect("node mutex poisoned");
                let (mut reply, messages) = runtime.node.handle_client_request(request);
                if let Err(err) = runtime.persist_if_changed(&config.state_path) {
                    eprintln!("persist client write: {err}");
                    reply.success = false;
                    reply.response = None;
                }
                (reply, messages)
            };
            write_reply_frame(&mut stream, reply)?;
            send_all(&config.peers, messages);
        }
        WireMessage::ClientReply(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected ClientReply from peer or client",
            ));
        }
    }
    Ok(())
}

fn send_all(peers: &HashMap<NodeId, String>, messages: Vec<raft_kv::Message>) {
    let mut per_peer: HashMap<NodeId, Vec<raft_kv::Message>> = HashMap::new();
    for message in messages {
        per_peer.entry(message.to).or_default().push(message);
    }
    for (peer, msgs) in per_peer {
        let Some(addr) = peers.get(&peer) else {
            continue;
        };
        match TcpStream::connect(addr) {
            Ok(mut stream) => {
                for message in msgs {
                    if let Err(err) = write_peer_frame(&mut stream, message) {
                        eprintln!("send to {addr}: {err}");
                        break;
                    }
                }
            }
            Err(err) => eprintln!("connect to {addr}: {err}"),
        }
    }
}

struct Runtime {
    node: Node,
    started: Instant,
    last_saved: DurableState,
}

impl Runtime {
    /// Persists if durable state differs from the last saved snapshot.
    /// Compares fields directly — no log allocation on the fast path.
    fn persist_if_changed(&mut self, path: &Path) -> io::Result<()> {
        let needs_save = self.node.current_term() != self.last_saved.current_term
            || self.node.voted_for() != self.last_saved.voted_for
            || self.node.commit_index() != self.last_saved.commit_index
            || self.node.log() != self.last_saved.log.as_slice();
        if needs_save {
            save_node(path, &self.node)?;
            self.last_saved = DurableState::from_node(&self.node);
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct Config {
    id: NodeId,
    peers: HashMap<NodeId, String>,
    state_path: PathBuf,
}

impl Config {
    fn from_args(args: Vec<String>) -> io::Result<Self> {
        if args.len() < 5 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: raft-node <id> <state-file> <id=addr>...",
            ));
        }
        let id = args[1]
            .parse()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid node id"))?;
        let state_path = PathBuf::from(&args[2]);
        let mut peers = HashMap::new();
        for peer in &args[3..] {
            let (id, addr) = peer.split_once('=').ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "peer must be id=addr")
            })?;
            let id = id
                .parse()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid peer id"))?;
            peers.insert(id, addr.to_string());
        }
        Ok(Self {
            id,
            peers,
            state_path,
        })
    }
}
