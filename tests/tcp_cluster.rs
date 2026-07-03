use raft_kv::net::{WireMessage, read_frame, write_frame};
use raft_kv::{ClientReply, ClientRequest, Command, NodeId};
use std::collections::HashMap;
use std::io;
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command as ProcessCommand, Stdio};
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn tcp_process_cluster_elects_replicates_and_recovers_after_leader_kill() {
    let dir = tempfile::tempdir().unwrap();
    let peers = reserve_ports(3);
    let mut children = spawn_cluster(dir.path(), &peers);

    let leader = wait_for_leader(&peers, Duration::from_secs(5)).expect("leader elected");
    let reply = send_client(
        &peers[&leader],
        Command::Set {
            key: "foo".to_string(),
            value: "bar".to_string(),
        },
    )
    .expect("set through leader");
    assert!(reply.success);
    assert_eq!(reply.leader_id, Some(leader));
    assert_eq!(
        wait_for_get(&peers, "foo", Duration::from_secs(5)),
        Some("bar".to_string())
    );

    children.get_mut(&leader).unwrap().kill().unwrap();
    let _ = children.get_mut(&leader).unwrap().wait();

    let new_leader =
        wait_for_leader_except(&peers, leader, Duration::from_secs(5)).expect("new leader elected");
    let reply = send_client(
        &peers[&new_leader],
        Command::Set {
            key: "baz".to_string(),
            value: "qux".to_string(),
        },
    )
    .expect("set through new leader");
    assert!(reply.success);
    assert_eq!(
        wait_for_get(&peers, "baz", Duration::from_secs(5)),
        Some("qux".to_string())
    );

    let restarted = spawn_node(dir.path(), leader, &peers);
    children.insert(leader, restarted);
    assert_eq!(
        wait_for_get(&peers, "foo", Duration::from_secs(5)),
        Some("bar".to_string())
    );

    for (_, mut child) in children {
        let _ = child.kill();
        let _ = child.wait();
    }
}

fn reserve_ports(count: usize) -> HashMap<NodeId, String> {
    let listeners: Vec<_> = (0..count)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    listeners
        .iter()
        .enumerate()
        .map(|(id, listener)| (id, listener.local_addr().unwrap().to_string()))
        .collect()
}

fn spawn_cluster(dir: &Path, peers: &HashMap<NodeId, String>) -> HashMap<NodeId, Child> {
    peers
        .keys()
        .copied()
        .map(|id| (id, spawn_node(dir, id, peers)))
        .collect()
}

fn spawn_node(dir: &Path, id: NodeId, peers: &HashMap<NodeId, String>) -> Child {
    let mut args = vec![
        id.to_string(),
        dir.join(format!("node-{id}.bin")).display().to_string(),
    ];
    let mut peer_args: Vec<_> = peers
        .iter()
        .map(|(id, addr)| format!("{id}={addr}"))
        .collect();
    peer_args.sort();
    args.extend(peer_args);
    ProcessCommand::new(env!("CARGO_BIN_EXE_raft-node"))
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap()
}

fn wait_for_leader(peers: &HashMap<NodeId, String>, timeout: Duration) -> Option<NodeId> {
    wait_for_leader_where(peers, timeout, |_| true)
}

fn wait_for_leader_except(
    peers: &HashMap<NodeId, String>,
    old: NodeId,
    timeout: Duration,
) -> Option<NodeId> {
    wait_for_leader_where(peers, timeout, |id| id != old)
}

fn wait_for_leader_where(
    peers: &HashMap<NodeId, String>,
    timeout: Duration,
    accept: impl Fn(NodeId) -> bool,
) -> Option<NodeId> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        for addr in peers.values() {
            if let Ok(reply) = send_client(
                addr,
                Command::Get {
                    key: "__probe__".to_string(),
                },
            ) && let Some(id) = reply.leader_id.filter(|&id| accept(id))
            {
                return Some(id);
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    None
}

fn wait_for_get(peers: &HashMap<NodeId, String>, key: &str, timeout: Duration) -> Option<String> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        for addr in peers.values() {
            if let Ok(reply) = send_client(
                addr,
                Command::Get {
                    key: key.to_string(),
                },
            ) && reply.success
                && reply.response.is_some()
            {
                return reply.response;
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    None
}

fn send_client(addr: &str, command: Command) -> io::Result<ClientReply> {
    let mut stream = TcpStream::connect(addr)?;
    stream.set_read_timeout(Some(Duration::from_secs(1)))?;
    stream.set_write_timeout(Some(Duration::from_secs(1)))?;
    write_frame(&mut stream, &WireMessage::Client(ClientRequest { command }))?;
    match read_frame(&mut stream)? {
        WireMessage::ClientReply(reply) => Ok(reply),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "expected client reply",
        )),
    }
}
