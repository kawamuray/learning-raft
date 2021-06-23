use crate::node::ops::Operation;
use log::debug;

#[derive(Debug, Clone, Copy)]
pub struct LogEntry {
    pub term: u64,
    pub op: Operation,
}

#[derive(Debug)]
pub struct Log {
    entries: Vec<LogEntry>,
    commit_index: usize,
    last_applied: usize,
}

impl Log {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            commit_index: 0,
            last_applied: 0,
        }
    }

    pub fn last_index(&self) -> usize {
        self.entries.len()
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
            EntryId { index: 0, term: 0 }
        }
    }

    pub fn append(&mut self, entry: LogEntry) {
        self.entries.push(entry);
    }

    pub fn get(&self, index: usize) -> Option<&LogEntry> {
        if index == 0 {
            return None;
        }
        self.entries.get(index - 1)
    }

    pub fn truncate_to(&mut self, index: usize) {
        self.entries.truncate(index);
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
        &self.entries[(from - 1)..]
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct EntryId {
    pub index: usize,
    pub term: u64,
}
