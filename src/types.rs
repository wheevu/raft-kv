use serde::{Deserialize, Serialize};

pub type NodeId = usize;
pub type Term = u64;
pub type LogIndex = usize;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Command {
    Noop,
    Set { key: String, value: String },
    Delete { key: String },
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
pub enum ClientRequest {
    Get { key: String },
    Set { key: String, value: String },
    Delete { key: String },
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
