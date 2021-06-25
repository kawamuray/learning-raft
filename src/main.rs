use std::io::{self, BufRead};
#[cfg(test)]
mod it;
mod log;
mod node;
mod transport;
use env_logger;
use node::Transport;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

const NUM_NODES: usize = 3;

fn main() {
    env_logger::init();

    let node_ids: Vec<_> = (1..=NUM_NODES).map(|id| id as u32).collect();
    let transport = Arc::new(Mutex::new(transport::InMemoryTransport::new()));

    let (super_tx, super_rx) = mpsc::channel();
    transport.lock().unwrap().add_super_node(super_tx);
    for i in 1..=NUM_NODES {
        let (tx, rx) = mpsc::channel();
        transport.lock().unwrap().add_node(tx);
        let mut node = node::RaftNode::new(i as u32, node_ids.clone(), Arc::clone(&transport));
        thread::spawn(move || {
            node.run(rx);
        });
    }

    let stdin = io::stdin();
    let mut lines = stdin.lock().lines();
    loop {
        eprint!("> ");
        if let Some(line) = lines.next() {
            let cmd: Vec<_> = line.unwrap().split(" ").map(|x| x.to_owned()).collect();
            match cmd[0].as_str() {
                "set" => {
                    // set 5 10 <= set 10 through node 5
                    // set value
                    if cmd.len() != 3 {
                        eprintln!("usage: set NODE VALUE");
                        continue;
                    }
                    let node: u32 = cmd[1].parse().unwrap();
                    let value: i32 = cmd[2].parse().unwrap();
                    transport.lock().unwrap().send(
                        0,
                        node,
                        node::Message::UpdateValue(node::ops::Operation::Set(value)),
                    );
                    let (from, msg) = super_rx.recv().unwrap();
                    if from != node {
                        panic!("???");
                    }
                    if let node::Message::UpdateValueResult(err) = msg {
                        match err {
                            Some(msg) => eprintln!("ERROR: {}", msg),
                            None => eprintln!("OK"),
                        }
                    } else {
                        eprintln!("unexpected message received: {:?}", msg);
                    }
                }
                "down" => {
                    let mut tp = transport.lock().unwrap();
                    for id in &cmd[1..] {
                        let id = id.parse::<u32>().unwrap();
                        eprintln!("down {}", id);
                        tp.down(id);
                    }
                }
                "up" => {
                    let mut tp = transport.lock().unwrap();
                    for id in &cmd[1..] {
                        let id = id.parse::<u32>().unwrap();
                        eprintln!("up {}", id);
                        tp.up(id);
                    }
                }
                "show" => {
                    let mut tp = transport.lock().unwrap();
                    // show current cluster status
                    for id in &node_ids {
                        tp.send(0, *id, node::Message::ShowStatus);
                    }
                    drop(tp);
                    let mut stats = vec![None; node_ids.len()];
                    for _ in 0..node_ids.len() {
                        let (from, msg) = super_rx.recv().unwrap();
                        match msg {
                            node::Message::Status(status) => {
                                stats[from as usize - 1] = Some(status);
                            }
                            msg => eprintln!("Unexpected message received: {:?}", msg),
                        }
                    }
                    for (i, status) in stats.into_iter().enumerate() {
                        let status = status.unwrap();
                        eprintln!(
                            "{}[{}] Term:{}, Leader:{}, ETO:{}, Value:{}",
                            i + 1,
                            status.state,
                            status.term,
                            status.leader,
                            status.election_timeout.as_secs(),
                            status.value,
                        );
                    }
                }
                c => {
                    eprintln!("no such command: {}", c);
                }
            }
        } else {
            break;
        }
    }
}
