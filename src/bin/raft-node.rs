use raft_kv::net::{WireMessage, read_frame, write_frame};
use raft_kv::storage::{load_node, save_node};
use raft_kv::{ClientReply, Node, NodeId};
use std::collections::HashMap;
use std::env;
use std::io;
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
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
    let shared = Arc::new(Mutex::new(Runtime {
        node,
        started: Instant::now(),
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
                if let Err(err) = save_node(&tick_config.state_path, &runtime.node) {
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
                save_node(&config.state_path, &runtime.node)?;
                replies
            };
            send_all(&config.peers, replies);
        }
        WireMessage::Client(request) => {
            let (reply, messages) = {
                let mut runtime = shared.lock().expect("node mutex poisoned");
                let (reply, messages) = runtime.node.handle_client_request(request);
                save_node(&config.state_path, &runtime.node)?;
                (reply, messages)
            };
            write_frame(&mut stream, &WireMessage::ClientReply(reply))?;
            send_all(&config.peers, messages);
        }
        WireMessage::ClientReply(_) => {}
    }
    Ok(())
}

fn send_all(peers: &HashMap<NodeId, String>, messages: Vec<raft_kv::Message>) {
    for message in messages {
        let Some(addr) = peers.get(&message.to) else {
            continue;
        };
        match TcpStream::connect(addr) {
            Ok(mut stream) => {
                let _ = write_frame(&mut stream, &WireMessage::Peer(message));
            }
            Err(err) => eprintln!("send to {addr}: {err}"),
        }
    }
}

struct Runtime {
    node: Node,
    started: Instant,
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

#[allow(dead_code)]
fn redirect_reply(node: &Node) -> ClientReply {
    ClientReply {
        success: false,
        leader_id: node.leader_id(),
        response: None,
    }
}
