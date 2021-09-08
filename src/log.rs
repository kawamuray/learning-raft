use crate::node;
use crate::node::ops::Operation;
use log::debug;

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub term: u64,
    pub op: Operation,
    pub config_update: Option<node::ClusterConfig>,
}

#[derive(Debug)]
pub struct Log {
    entries: Vec<LogEntry>,
    pub base_index: usize,
    base_term: u64,
    commit_index: usize,
    last_applied: usize,
}

impl Log {
    pub fn new(base_index: usize, base_term: u64) -> Self {
        Self {
            entries: Vec::new(),
            base_index,
            base_term,
            commit_index: base_index,
            last_applied: base_index,
        }
    }

    pub fn size(&self) -> usize {
        self.entries.len()
    }

    fn pos(&self, index: usize) -> usize {
        index - self.base_index - 1
    }

    pub fn last_index(&self) -> usize {
        self.base_index + self.entries.len()
    }

    pub fn committed_index(&self) -> usize {
        self.commit_index
    }

    pub fn last_entry_id(&self) -> EntryId {
        self.entry_id_at(self.last_index())
    }

    pub fn entry_id_at(&self, index: usize) -> EntryId {
        if let Some(entry) = self.get(index) {
            EntryId {
                index,
                term: entry.term,
            }
        } else {
            EntryId {
                index: self.base_index,
                term: self.base_term,
            }
        }
    }

    pub fn append(&mut self, entry: LogEntry) -> usize {
        self.entries.push(entry);
        self.last_index()
    }

    pub fn get(&self, index: usize) -> Option<&LogEntry> {
        if index <= self.base_index {
            return None;
        }
        self.entries.get(self.pos(index))
    }

    pub fn truncate_to(&mut self, index: usize) {
        self.entries.truncate(index - self.base_index);
    }

    pub fn drop_until(&mut self, index: usize) {
        self.entries.drain(0..=self.pos(index));
        self.base_index = index;
    }

    pub fn commit(&mut self, index: usize, initial_value: i32) -> i32 {
        self.commit_index = index.min(self.last_index());

        let mut value = initial_value;
        for i in (self.last_applied + 1)..=self.commit_index {
            value = self.get(i).unwrap().op.apply(value);
            self.last_applied = i;
            debug!("Apply commit {} to value, becomes {}", i, value);
        }
        value
    }

    pub fn entries_from(&self, from: usize) -> &[LogEntry] {
        &self.entries[self.pos(from)..]
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct EntryId {
    pub term: u64,
    pub index: usize,
}

#[derive(Clone, Debug)]
pub struct Snapshot {
    pub last_index: usize,
    pub last_term: u64,
    pub value: i32,
    pub config: node::ClusterConfig,
}

impl Snapshot {
    pub fn new(last_index: usize, last_term: u64, value: i32, config: node::ClusterConfig) -> Self {
        Self {
            last_index,
            last_term,
            value,
            config,
        }
    }
}
