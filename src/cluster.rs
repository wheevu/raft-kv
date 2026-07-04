use crate::node::Node;
use crate::types::*;
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Debug)]
pub struct Cluster {
    nodes: HashMap<NodeId, Node>,
    messages: VecDeque<Message>,
    now_ms: u64,
    stopped: HashMap<NodeId, bool>,
    blocked: HashSet<(NodeId, NodeId)>,
}

impl Cluster {
    pub fn new(size: usize) -> Self {
        let ids: Vec<_> = (0..size).collect();
        let nodes = ids
            .iter()
            .map(|&id| {
                let peers = ids.iter().copied().filter(|&peer| peer != id).collect();
                (id, Node::new(id, peers))
            })
            .collect();
        Self {
            nodes,
            messages: VecDeque::new(),
            now_ms: 0,
            stopped: HashMap::new(),
            blocked: HashSet::new(),
        }
    }

    pub fn node(&self, id: NodeId) -> &Node {
        &self.nodes[&id]
    }

    pub fn nodes(&self) -> impl Iterator<Item = (NodeId, &Node)> {
        self.nodes.iter().map(|(&id, node)| (id, node))
    }

    pub fn node_ids(&self) -> impl Iterator<Item = NodeId> {
        self.nodes.keys().copied()
    }

    pub fn stop(&mut self, id: NodeId) {
        self.stopped.insert(id, true);
    }

    pub fn partition(&mut self, groups: &[Vec<NodeId>]) {
        self.blocked.clear();
        let ids: Vec<_> = self.nodes.keys().copied().collect();
        for &from in &ids {
            for &to in &ids {
                if from == to {
                    continue;
                }
                let connected = groups
                    .iter()
                    .any(|group| group.contains(&from) && group.contains(&to));
                if !connected {
                    self.blocked.insert((from, to));
                }
            }
        }
    }

    pub fn heal(&mut self) {
        self.blocked.clear();
    }

    pub fn leader(&self) -> Option<NodeId> {
        let leaders: Vec<_> = self
            .nodes
            .values()
            .filter(|node| !self.is_stopped(node.id()) && node.role() == Role::Leader)
            .map(|node| node.id())
            .collect();
        if leaders.len() == 1 {
            Some(leaders[0])
        } else {
            None
        }
    }

    pub fn propose(&mut self, leader: NodeId, request: ClientRequest) -> ClientReply {
        if self.is_stopped(leader) {
            return ClientReply {
                success: false,
                leader_id: None,
                response: None,
            };
        }
        let (reply, messages) = self
            .nodes
            .get_mut(&leader)
            .unwrap()
            .handle_client_request(request);
        self.enqueue(messages);
        reply
    }

    pub fn run_until<F>(&mut self, deadline_ms: u64, mut predicate: F) -> bool
    where
        F: FnMut(&Self) -> bool,
    {
        while self.now_ms <= deadline_ms {
            if predicate(self) {
                return true;
            }
            self.step();
        }
        predicate(self)
    }

    pub fn run_for(&mut self, duration_ms: u64) {
        let deadline = self.now_ms + duration_ms;
        while self.now_ms <= deadline {
            self.step();
        }
    }

    pub fn now(&self) -> u64 {
        self.now_ms
    }

    fn step(&mut self) {
        if let Some(message) = self.messages.pop_front() {
            if self.is_stopped(message.from)
                || self.is_stopped(message.to)
                || self.is_blocked(message.from, message.to)
            {
                return;
            }
            let replies = self.nodes.get_mut(&message.to).unwrap().handle_message(
                message.from,
                message.rpc,
                self.now_ms,
            );
            self.enqueue(replies);
            return;
        }
        self.now_ms += 1;
        let ids: Vec<_> = self.nodes.keys().copied().collect();
        for id in ids {
            if self.is_stopped(id) {
                continue;
            }
            let messages = self.nodes.get_mut(&id).unwrap().tick(self.now_ms);
            self.enqueue(messages);
        }
    }

    fn enqueue(&mut self, messages: Vec<Message>) {
        for message in messages {
            if !self.is_stopped(message.from)
                && !self.is_stopped(message.to)
                && !self.is_blocked(message.from, message.to)
            {
                self.messages.push_back(message);
            }
        }
    }

    fn is_stopped(&self, id: NodeId) -> bool {
        self.stopped.get(&id).copied().unwrap_or(false)
    }

    fn is_blocked(&self, from: NodeId, to: NodeId) -> bool {
        self.blocked.contains(&(from, to))
    }
}
