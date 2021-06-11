use crate::node::{Message, Transport};
use std::sync::mpsc;

struct Node {
    tx: mpsc::Sender<(u32, Message)>,
    up: bool,
    is_super: bool,
}

impl Node {
    fn new(tx: mpsc::Sender<(u32, Message)>, is_super: bool) -> Self {
        Self {
            tx,
            up: true,
            is_super,
        }
    }
}

pub struct InMemoryTransport {
    nodes: Vec<Node>,
}

impl InMemoryTransport {
    pub fn new() -> Self {
        Self { nodes: Vec::new() }
    }

    pub fn add_node(&mut self, tx: mpsc::Sender<(u32, Message)>) {
        self.nodes.push(Node::new(tx, false));
    }

    pub fn add_super_node(&mut self, tx: mpsc::Sender<(u32, Message)>) {
        self.nodes.push(Node::new(tx, true));
    }

    pub fn down(&mut self, node: u32) {
        self.nodes[node as usize].up = false;
    }

    pub fn up(&mut self, node: u32) {
        self.nodes[node as usize].up = true;
    }
}

impl Transport for InMemoryTransport {
    fn send(&mut self, from: u32, to: u32, msg: crate::node::Message) {
        let is_super = self.nodes[from as usize].is_super || self.nodes[to as usize].is_super;
        if !is_super && !self.nodes[from as usize].up {
            return;
        }
        let node = &mut self.nodes[to as usize];
        if !is_super && !node.up {
            return;
        }
        node.tx.send((from, msg)).expect("send message to a node");
    }
}
