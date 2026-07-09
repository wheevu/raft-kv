use crate::state_machine::{MemoryStateMachine, StateMachine};
use crate::types::*;
use std::collections::HashMap;

const HEARTBEAT_MS: u64 = 50;
const ELECTION_MIN_MS: u64 = 150;
const ELECTION_SPAN_MS: u64 = 151;

#[derive(Clone, Debug)]
pub struct Node<S: StateMachine = MemoryStateMachine> {
    id: NodeId,
    peers: Vec<NodeId>,
    role: Role,
    current_term: Term,
    voted_for: Option<NodeId>,
    log: Vec<LogEntry>,
    commit_index: LogIndex,
    last_applied: LogIndex,
    state_machine: S,
    leader_id: Option<NodeId>,
    election_deadline: u64,
    last_heartbeat_at: u64,
    rng_state: u64,
    votes_granted: usize,
    next_index: HashMap<NodeId, LogIndex>,
    match_index: HashMap<NodeId, LogIndex>,
    /// Index of the first log entry in the current term (noop). When commit_index
    /// reaches this, the leader has proved it is still the leader and can serve reads.
    term_start_index: LogIndex,
}

impl Node<MemoryStateMachine> {
    pub fn new(id: NodeId, peers: Vec<NodeId>) -> Self {
        Self::new_with_state_machine(id, peers, MemoryStateMachine::new())
    }

    pub fn from_parts(
        id: NodeId,
        peers: Vec<NodeId>,
        current_term: Term,
        voted_for: Option<NodeId>,
        log: Vec<LogEntry>,
        commit_index: LogIndex,
    ) -> Self {
        Self::from_parts_with_state_machine(
            id,
            peers,
            current_term,
            voted_for,
            log,
            commit_index,
            MemoryStateMachine::new(),
        )
    }
}

impl<S: StateMachine> Node<S> {
    pub fn new_with_state_machine(id: NodeId, peers: Vec<NodeId>, state_machine: S) -> Self {
        let mut node = Self {
            id,
            peers,
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: state_machine.last_applied(),
            state_machine,
            leader_id: None,
            election_deadline: 0,
            last_heartbeat_at: 0,
            rng_state: (id as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15),
            votes_granted: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            term_start_index: 0,
        };
        node.reset_election_timer(0);
        node
    }

    pub fn from_parts_with_state_machine(
        id: NodeId,
        peers: Vec<NodeId>,
        current_term: Term,
        voted_for: Option<NodeId>,
        log: Vec<LogEntry>,
        commit_index: LogIndex,
        state_machine: S,
    ) -> Self {
        let mut node = Self::new_with_state_machine(id, peers, state_machine);
        node.current_term = current_term;
        node.voted_for = voted_for;
        node.log = log;
        node.commit_index = commit_index.min(node.log.len());
        let _ = node.apply_committed();
        node
    }

    pub fn id(&self) -> NodeId {
        self.id
    }
    pub fn leader_id(&self) -> Option<NodeId> {
        self.leader_id
    }
    pub fn role(&self) -> Role {
        self.role
    }
    pub fn current_term(&self) -> Term {
        self.current_term
    }
    pub fn voted_for(&self) -> Option<NodeId> {
        self.voted_for
    }
    pub fn log(&self) -> &[LogEntry] {
        &self.log
    }
    pub fn commit_index(&self) -> LogIndex {
        self.commit_index
    }
    pub fn last_applied(&self) -> LogIndex {
        self.last_applied
    }
    pub fn can_serve_reads(&self) -> bool {
        self.commit_index >= self.term_start_index
    }
    pub fn get(&self, key: &str) -> Option<String> {
        self.state_machine.get(key).ok().flatten()
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
        if let ClientRequest::Get { key } = &request {
            if !self.can_serve_reads() {
                return (
                    ClientReply {
                        success: false,
                        leader_id: Some(self.id),
                        response: None,
                    },
                    Vec::new(),
                );
            }
            return (
                ClientReply {
                    success: true,
                    leader_id: Some(self.id),
                    response: match self.state_machine.get(key) {
                        Ok(value) => value,
                        Err(_) => return (ClientReply { success: false, leader_id: Some(self.id), response: None }, Vec::new()),
                    },
                },
                Vec::new(),
            );
        }
        let command = match request {
            ClientRequest::Set { key, value } => Command::Set { key, value },
            ClientRequest::Delete { key } => Command::Delete { key },
            ClientRequest::Get { .. } => unreachable!(),
        };
        self.log.push(LogEntry {
            term: self.current_term,
            command,
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
        self.term_start_index = self.last_log_index();
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
        let _ = self.apply_committed();
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
        let _ = self.apply_committed();
    }

    fn apply_committed(&mut self) -> std::io::Result<()> {
        while self.last_applied < self.commit_index {
            let next = self.last_applied + 1;
            self.state_machine
                .apply(next, &self.log[next - 1].command)?;
            self.last_applied = next;
        }
        Ok(())
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
    pub fn last_log_index(&self) -> LogIndex {
        self.log.len()
    }
    fn last_log_term(&self) -> Term {
        self.log.last().map_or(0, |entry| entry.term)
    }
    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index == 0 {
            Some(0)
        } else {
            self.log.get(index - 1).map(|entry| entry.term)
        }
    }
}
