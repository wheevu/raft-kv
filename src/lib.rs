use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};

pub mod net;
pub mod storage;

pub type NodeId = usize;
pub type Term = u64;
pub type LogIndex = usize;

const HEARTBEAT_MS: u64 = 50;
const ELECTION_MIN_MS: u64 = 150;
const ELECTION_SPAN_MS: u64 = 151;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Command {
    Noop,
    Get { key: String },
    Set { key: String, value: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LogEntry {
    pub term: Term,
    pub command: Command,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RequestVote {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RequestVoteReply {
    pub term: Term,
    pub vote_granted: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AppendEntries {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry>,
    pub leader_commit: LogIndex,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AppendEntriesReply {
    pub term: Term,
    pub success: bool,
    pub match_index: LogIndex,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ClientRequest {
    pub command: Command,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ClientReply {
    pub success: bool,
    pub leader_id: Option<NodeId>,
    pub response: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Rpc {
    RequestVote(RequestVote),
    RequestVoteReply(RequestVoteReply),
    AppendEntries(AppendEntries),
    AppendEntriesReply(AppendEntriesReply),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Message {
    pub from: NodeId,
    pub to: NodeId,
    pub rpc: Rpc,
}

#[derive(Clone, Debug)]
pub struct Node {
    id: NodeId,
    peers: Vec<NodeId>,
    pub role: Role,
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub log: Vec<LogEntry>,
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,
    pub state_machine: HashMap<String, String>,
    leader_id: Option<NodeId>,
    election_deadline: u64,
    last_heartbeat_at: u64,
    rng_state: u64,
    votes_granted: usize,
    next_index: HashMap<NodeId, LogIndex>,
    match_index: HashMap<NodeId, LogIndex>,
}

impl Node {
    pub fn new(id: NodeId, peers: Vec<NodeId>) -> Self {
        let mut node = Self {
            id,
            peers,
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            state_machine: HashMap::new(),
            leader_id: None,
            election_deadline: 0,
            last_heartbeat_at: 0,
            rng_state: (id as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15),
            votes_granted: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
        };
        node.reset_election_timer(0);
        node
    }

    pub fn from_parts(
        id: NodeId,
        peers: Vec<NodeId>,
        current_term: Term,
        voted_for: Option<NodeId>,
        log: Vec<LogEntry>,
        commit_index: LogIndex,
    ) -> Self {
        let mut node = Self::new(id, peers);
        node.current_term = current_term;
        node.voted_for = voted_for;
        node.log = log;
        node.commit_index = commit_index.min(node.log.len());
        node.apply_committed();
        node
    }

    pub fn id(&self) -> NodeId {
        self.id
    }
    pub fn leader_id(&self) -> Option<NodeId> {
        self.leader_id
    }
    pub fn get(&self, key: &str) -> Option<&str> {
        self.state_machine.get(key).map(String::as_str)
    }

    pub fn peers(&self) -> &[NodeId] {
        &self.peers
    }

    pub fn tick(&mut self, now_ms: u64) -> Vec<Message> {
        match self.role {
            Role::Leader if now_ms.saturating_sub(self.last_heartbeat_at) >= HEARTBEAT_MS => {
                self.last_heartbeat_at = now_ms;
                self.peers
                    .iter()
                    .map(|&peer| self.append_entries_for(peer))
                    .collect()
            }
            Role::Follower | Role::Candidate if now_ms >= self.election_deadline => {
                self.start_election(now_ms)
            }
            _ => Vec::new(),
        }
    }

    pub fn handle_message(&mut self, from: NodeId, rpc: Rpc, now_ms: u64) -> Vec<Message> {
        match rpc {
            Rpc::RequestVote(v) => vec![Message {
                from: self.id,
                to: from,
                rpc: Rpc::RequestVoteReply(self.handle_request_vote(v, now_ms)),
            }],
            Rpc::RequestVoteReply(r) => self.handle_request_vote_reply(from, r, now_ms),
            Rpc::AppendEntries(a) => vec![Message {
                from: self.id,
                to: from,
                rpc: Rpc::AppendEntriesReply(self.handle_append_entries(a, now_ms)),
            }],
            Rpc::AppendEntriesReply(r) => self.handle_append_entries_reply(from, r),
        }
    }

    pub fn handle_client_request(&mut self, request: ClientRequest) -> (ClientReply, Vec<Message>) {
        if self.role != Role::Leader {
            return (
                ClientReply {
                    success: false,
                    leader_id: self.leader_id,
                    response: None,
                },
                Vec::new(),
            );
        }
        if let Command::Get { key } = &request.command {
            return (
                ClientReply {
                    success: true,
                    leader_id: Some(self.id),
                    response: self.state_machine.get(key).cloned(),
                },
                Vec::new(),
            );
        }
        self.log.push(LogEntry {
            term: self.current_term,
            command: request.command,
        });
        let last = self.last_log_index();
        self.match_index.insert(self.id, last);
        let messages = self
            .peers
            .iter()
            .map(|&peer| self.append_entries_for(peer))
            .collect();
        (
            ClientReply {
                success: true,
                leader_id: Some(self.id),
                response: Some("accepted".to_string()),
            },
            messages,
        )
    }

    fn start_election(&mut self, now_ms: u64) -> Vec<Message> {
        self.role = Role::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.leader_id = None;
        self.votes_granted = 1;
        self.reset_election_timer(now_ms);
        let request = RequestVote {
            term: self.current_term,
            candidate_id: self.id,
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        };
        self.peers
            .iter()
            .map(|&peer| Message {
                from: self.id,
                to: peer,
                rpc: Rpc::RequestVote(request.clone()),
            })
            .collect()
    }

    fn handle_request_vote(&mut self, request: RequestVote, now_ms: u64) -> RequestVoteReply {
        if request.term < self.current_term {
            return RequestVoteReply {
                term: self.current_term,
                vote_granted: false,
            };
        }
        if request.term > self.current_term {
            self.step_down(request.term, now_ms);
        }
        let can_vote = self.voted_for.is_none() || self.voted_for == Some(request.candidate_id);
        let up_to_date = request.last_log_term > self.last_log_term()
            || (request.last_log_term == self.last_log_term()
                && request.last_log_index >= self.last_log_index());
        let vote_granted = can_vote && up_to_date;
        if vote_granted {
            self.voted_for = Some(request.candidate_id);
            self.reset_election_timer(now_ms);
        }
        RequestVoteReply {
            term: self.current_term,
            vote_granted,
        }
    }

    fn handle_request_vote_reply(
        &mut self,
        _from: NodeId,
        reply: RequestVoteReply,
        now_ms: u64,
    ) -> Vec<Message> {
        if reply.term > self.current_term {
            self.step_down(reply.term, now_ms);
            return Vec::new();
        }
        if self.role != Role::Candidate || reply.term != self.current_term || !reply.vote_granted {
            return Vec::new();
        }
        self.votes_granted += 1;
        if self.votes_granted >= self.majority() {
            self.become_leader(now_ms)
        } else {
            Vec::new()
        }
    }

    fn become_leader(&mut self, now_ms: u64) -> Vec<Message> {
        self.role = Role::Leader;
        self.leader_id = Some(self.id);
        self.last_heartbeat_at = now_ms;
        self.log.push(LogEntry {
            term: self.current_term,
            command: Command::Noop,
        });
        let next = self.last_log_index() + 1;
        self.next_index = self.peers.iter().map(|&peer| (peer, next)).collect();
        self.match_index = self.peers.iter().map(|&peer| (peer, 0)).collect();
        self.match_index.insert(self.id, self.last_log_index());
        self.peers
            .iter()
            .map(|&peer| self.append_entries_for(peer))
            .collect()
    }

    fn handle_append_entries(&mut self, request: AppendEntries, now_ms: u64) -> AppendEntriesReply {
        if request.term < self.current_term {
            return AppendEntriesReply {
                term: self.current_term,
                success: false,
                match_index: self.last_log_index(),
            };
        }
        if request.term > self.current_term || self.role != Role::Follower {
            self.step_down(request.term, now_ms);
        }
        self.leader_id = Some(request.leader_id);
        self.reset_election_timer(now_ms);
        if self.term_at(request.prev_log_index) != Some(request.prev_log_term) {
            return AppendEntriesReply {
                term: self.current_term,
                success: false,
                match_index: self.last_log_index(),
            };
        }
        let mut index = request.prev_log_index + 1;
        for entry in request.entries {
            if self.term_at(index).is_some_and(|term| term != entry.term) {
                self.log.truncate(index - 1);
            }
            if self.term_at(index).is_none() {
                self.log.push(entry);
            }
            index += 1;
        }
        if request.leader_commit > self.commit_index {
            self.commit_index = request.leader_commit.min(self.last_log_index());
        }
        self.apply_committed();
        AppendEntriesReply {
            term: self.current_term,
            success: true,
            match_index: index - 1,
        }
    }

    fn handle_append_entries_reply(
        &mut self,
        from: NodeId,
        reply: AppendEntriesReply,
    ) -> Vec<Message> {
        if reply.term > self.current_term {
            self.step_down(reply.term, 0);
            return Vec::new();
        }
        if self.role != Role::Leader || reply.term != self.current_term {
            return Vec::new();
        }
        if reply.success {
            self.match_index.insert(from, reply.match_index);
            self.next_index.insert(from, reply.match_index + 1);
            self.advance_commit_index();
            Vec::new()
        } else {
            let next = self
                .next_index
                .get(&from)
                .copied()
                .unwrap_or(self.last_log_index() + 1)
                .saturating_sub(1)
                .max(1);
            self.next_index.insert(from, next);
            vec![self.append_entries_for(from)]
        }
    }

    fn append_entries_for(&self, peer: NodeId) -> Message {
        let next = self
            .next_index
            .get(&peer)
            .copied()
            .unwrap_or(self.last_log_index() + 1);
        let prev_log_index = next.saturating_sub(1);
        let entries = self.log.iter().skip(next - 1).cloned().collect();
        Message {
            from: self.id,
            to: peer,
            rpc: Rpc::AppendEntries(AppendEntries {
                term: self.current_term,
                leader_id: self.id,
                prev_log_index,
                prev_log_term: self.term_at(prev_log_index).unwrap_or(0),
                entries,
                leader_commit: self.commit_index,
            }),
        }
    }

    fn advance_commit_index(&mut self) {
        for index in (self.commit_index + 1)..=self.last_log_index() {
            if self.term_at(index) == Some(self.current_term) {
                let replicated = self
                    .match_index
                    .values()
                    .filter(|&&matched| matched >= index)
                    .count();
                if replicated >= self.majority() {
                    self.commit_index = index;
                }
            }
        }
        self.apply_committed();
    }

    fn apply_committed(&mut self) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            match &self.log[self.last_applied - 1].command {
                Command::Noop => {}
                Command::Get { .. } => {}
                Command::Set { key, value } => {
                    self.state_machine.insert(key.clone(), value.clone());
                }
            }
        }
    }

    fn step_down(&mut self, term: Term, now_ms: u64) {
        self.role = Role::Follower;
        self.current_term = term;
        self.voted_for = None;
        self.votes_granted = 0;
        self.reset_election_timer(now_ms);
    }

    fn reset_election_timer(&mut self, now_ms: u64) {
        self.election_deadline = now_ms + self.next_election_timeout();
    }
    fn next_election_timeout(&mut self) -> u64 {
        self.rng_state = self
            .rng_state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1);
        ELECTION_MIN_MS + (self.rng_state % ELECTION_SPAN_MS)
    }
    fn majority(&self) -> usize {
        let cluster_size = self.peers.len() + 1;
        cluster_size / 2 + 1
    }
    fn last_log_index(&self) -> LogIndex {
        self.log.len()
    }
    fn last_log_term(&self) -> Term {
        self.log.last().map_or(0, |entry| entry.term)
    }
    fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index == 0 {
            Some(0)
        } else {
            self.log.get(index - 1).map(|entry| entry.term)
        }
    }
}

#[derive(Debug)]
pub struct Cluster {
    pub nodes: HashMap<NodeId, Node>,
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
            .filter(|node| !self.is_stopped(node.id) && node.role == Role::Leader)
            .map(Node::id)
            .collect();
        if leaders.len() == 1 {
            Some(leaders[0])
        } else {
            None
        }
    }

    pub fn propose(&mut self, leader: NodeId, command: Command) -> ClientReply {
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
            .handle_client_request(ClientRequest { command });
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_election() {
        let mut cluster = Cluster::new(5);
        assert!(cluster.run_until(600, |cluster| cluster.leader().is_some()));
        let leader = cluster.leader().unwrap();
        let term = cluster.nodes[&leader].current_term;
        assert!(term <= 2, "leader elected in term {term}");
        assert_eq!(
            cluster
                .nodes
                .values()
                .filter(|node| node.role == Role::Leader)
                .count(),
            1
        );
    }

    #[test]
    fn test_re_election() {
        let mut cluster = Cluster::new(5);
        assert!(cluster.run_until(600, |cluster| cluster.leader().is_some()));
        let old_leader = cluster.leader().unwrap();
        cluster.stop(old_leader);
        assert!(cluster.run_until(1000, |cluster| {
            cluster.leader().is_some_and(|leader| leader != old_leader)
        }));
    }

    #[test]
    fn test_basic_agree() {
        let mut cluster = Cluster::new(5);
        assert!(cluster.run_until(600, |cluster| cluster.leader().is_some()));
        let leader = cluster.leader().unwrap();
        let reply = cluster.propose(
            leader,
            Command::Set {
                key: "foo".to_string(),
                value: "bar".to_string(),
            },
        );
        assert!(reply.success);
        assert!(cluster.run_until(1000, |cluster| {
            cluster
                .nodes
                .values()
                .all(|node| node.get("foo") == Some("bar"))
        }));
    }

    #[test]
    fn test_fail_agree() {
        let mut cluster = Cluster::new(5);
        assert!(cluster.run_until(600, |cluster| cluster.leader().is_some()));
        cluster.run_for(200);
        let old_leader = cluster.leader().unwrap();
        let reply = cluster.propose(
            old_leader,
            Command::Set {
                key: "foo".to_string(),
                value: "bar".to_string(),
            },
        );
        assert!(reply.success);
        for _ in 0..8 {
            cluster.step();
        }
        cluster.stop(old_leader);
        assert!(cluster.run_until(1100, |cluster| {
            cluster.leader().is_some_and(|leader| leader != old_leader)
        }));
        assert!(cluster.run_until(3000, |cluster| {
            cluster
                .nodes
                .iter()
                .filter(|(id, _)| **id != old_leader)
                .all(|(_, node)| node.get("foo") == Some("bar"))
        }));
    }

    #[test]
    fn test_fail_no_agree() {
        let mut cluster = Cluster::new(5);
        assert!(cluster.run_until(600, |cluster| cluster.leader().is_some()));
        cluster.run_for(200);
        let old_leader = cluster.leader().unwrap();
        let minority = vec![old_leader];
        let majority: Vec<_> = (0..5).filter(|&id| id != old_leader).collect();
        cluster.partition(&[minority, majority.clone()]);

        let reply = cluster.propose(
            old_leader,
            Command::Set {
                key: "lost".to_string(),
                value: "value".to_string(),
            },
        );
        assert!(reply.success);
        cluster.run_for(500);
        assert!(cluster.nodes[&old_leader].get("lost").is_none());

        assert!(cluster.run_until(1200, |cluster| {
            majority
                .iter()
                .any(|id| cluster.nodes[id].role == Role::Leader)
        }));
        cluster.heal();
        assert!(cluster.run_until(2500, |cluster| {
            cluster
                .nodes
                .values()
                .filter(|node| node.id() != old_leader)
                .all(|node| node.get("lost").is_none())
        }));
    }

    #[test]
    fn test_concurrent_starts() {
        let mut cluster = Cluster::new(5);
        assert!(cluster.run_until(600, |cluster| cluster.leader().is_some()));
        let leader = cluster.leader().unwrap();
        for index in 0..10 {
            let reply = cluster.propose(
                leader,
                Command::Set {
                    key: format!("k{index}"),
                    value: format!("v{index}"),
                },
            );
            assert!(reply.success);
        }
        assert!(cluster.run_until(2500, |cluster| (0..10).all(|index| {
            cluster
                .nodes
                .values()
                .all(|node| node.get(&format!("k{index}")) == Some(format!("v{index}").as_str()))
        })));
    }

    #[test]
    fn test_rejoin() {
        let mut cluster = Cluster::new(5);
        assert!(cluster.run_until(600, |cluster| cluster.leader().is_some()));
        cluster.run_for(200);
        let isolated = (0..5).find(|&id| Some(id) != cluster.leader()).unwrap();
        let connected: Vec<_> = (0..5).filter(|&id| id != isolated).collect();
        cluster.partition(&[vec![isolated], connected]);
        let leader = cluster.leader().unwrap();
        let reply = cluster.propose(
            leader,
            Command::Set {
                key: "after_partition".to_string(),
                value: "ok".to_string(),
            },
        );
        assert!(reply.success);
        assert!(cluster.run_until(1500, |cluster| {
            cluster
                .nodes
                .iter()
                .filter(|(id, _)| **id != isolated)
                .all(|(_, node)| node.get("after_partition") == Some("ok"))
        }));
        assert!(cluster.nodes[&isolated].get("after_partition").is_none());

        cluster.heal();
        assert!(cluster.run_until(
            3000,
            |cluster| cluster.nodes[&isolated].get("after_partition") == Some("ok")
        ));
    }
}
