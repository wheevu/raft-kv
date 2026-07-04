use crate::{LogEntry, LogIndex, Node, NodeId, Term};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DurableState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub log: Vec<LogEntry>,
    pub commit_index: LogIndex,
}

impl DurableState {
    pub fn from_node(node: &Node) -> Self {
        Self {
            current_term: node.current_term(),
            voted_for: node.voted_for(),
            log: node.log().to_vec(),
            commit_index: node.commit_index(),
        }
    }
}

pub fn load_node(path: &Path, id: NodeId, peers: Vec<NodeId>) -> io::Result<Node> {
    match fs::read(path) {
        Ok(bytes) => {
            let state: DurableState = bincode::deserialize(&bytes)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
            Ok(Node::from_parts(
                id,
                peers,
                state.current_term,
                state.voted_for,
                state.log,
                state.commit_index,
            ))
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(Node::new(id, peers)),
        Err(err) => Err(err),
    }
}

pub fn save_node(path: &Path, node: &Node) -> io::Result<()> {
    save_state(path, &DurableState::from_node(node))
}

pub fn save_state(path: &Path, state: &DurableState) -> io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let tmp_path = tmp_path_for(path);
    let bytes = bincode::serialize(state).map_err(io::Error::other)?;
    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
    }
    fs::rename(&tmp_path, path)?;
    if let Ok(dir) = File::open(parent) {
        let _ = dir.sync_all();
    }
    Ok(())
}

fn tmp_path_for(path: &Path) -> PathBuf {
    let mut tmp = path.to_path_buf();
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("state.bin");
    tmp.set_file_name(format!(".{name}.tmp"));
    tmp
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Command;

    #[test]
    fn save_and_load_restores_term_vote_log_and_state_machine() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("node.bin");
        let node = Node::from_parts(
            0,
            vec![1, 2],
            4,
            Some(2),
            vec![LogEntry {
                term: 4,
                command: Command::Set {
                    key: "foo".to_string(),
                    value: "bar".to_string(),
                },
            }],
            1,
        );
        save_node(&path, &node).unwrap();
        let loaded = load_node(&path, 0, vec![1, 2]).unwrap();

        assert_eq!(loaded.current_term(), 4);
        assert_eq!(loaded.voted_for(), Some(2));
        assert_eq!(loaded.log(), node.log());
        assert_eq!(loaded.get("foo"), Some("bar"));
    }
}
