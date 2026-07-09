use crate::{Command, LogIndex};
use std::collections::HashMap;
use std::io;

pub trait StateMachine: std::fmt::Debug {
    fn apply(&mut self, index: LogIndex, command: &Command) -> io::Result<()>;
    fn get(&self, key: &str) -> io::Result<Option<String>>;
    fn last_applied(&self) -> LogIndex;
}

#[derive(Clone, Debug, Default)]
pub struct MemoryStateMachine {
    data: HashMap<String, String>,
    last_applied: LogIndex,
}

impl MemoryStateMachine {
    pub fn new() -> Self {
        Self::default()
    }
}

impl StateMachine for MemoryStateMachine {
    fn apply(&mut self, index: LogIndex, command: &Command) -> io::Result<()> {
        if index <= self.last_applied {
            return Ok(());
        }
        if index != self.last_applied + 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "state machine apply index gap",
            ));
        }
        match command {
            Command::Noop => {}
            Command::Set { key, value } => {
                self.data.insert(key.clone(), value.clone());
            }
            Command::Delete { key } => {
                self.data.remove(key);
            }
        }
        self.last_applied = index;
        Ok(())
    }

    fn get(&self, key: &str) -> io::Result<Option<String>> {
        Ok(self.data.get(key).cloned())
    }

    fn last_applied(&self) -> LogIndex {
        self.last_applied
    }
}
