use crate::node::{self, Transport};
use crate::transport::{self, InMemoryTransport};
use env_logger;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

const NUM_NODES: usize = 3;
const ELECTION_TIMEOUT_MIN: Duration = Duration::from_millis(150);
const ELECTION_TIMEOUT_MAX: Duration = Duration::from_millis(300);

struct SyncRpcClient<T: Transport> {
    transport: Arc<Mutex<T>>,
    rx: mpsc::Receiver<(u32, node::Message)>,
}

impl<T: Transport> SyncRpcClient<T> {
    fn new(transport: Arc<Mutex<T>>, rx: mpsc::Receiver<(u32, node::Message)>) -> Self {
        Self { transport, rx }
    }

    fn request(&self, to: u32, msg: node::Message) -> node::Message {
        self.transport.lock().unwrap().send(0, to, msg);
        let (from, msg) = self.rx.recv().unwrap();
        if from != to {
            panic!("unexpected response from {}", from);
        }
        msg
    }
}

struct TestingContext {
    node_ids: Vec<u32>,
    client: SyncRpcClient<InMemoryTransport>,
    transport: Arc<Mutex<InMemoryTransport>>,
}

impl TestingContext {
    fn new(num_nodes: usize) -> Self {
        let node_ids: Vec<_> = (1..=num_nodes).map(|id| id as u32).collect();
        let transport = Arc::new(Mutex::new(transport::InMemoryTransport::new()));

        let (super_tx, super_rx) = mpsc::channel();
        transport.lock().unwrap().add_super_node(super_tx);
        for i in 1..=num_nodes {
            let (tx, rx) = mpsc::channel();
            transport.lock().unwrap().add_node(tx);
            let mut node = node::RaftNode::new(i as u32, node_ids.clone(), Arc::clone(&transport));
            node.set_election_timeout(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX);
            thread::spawn(move || {
                node.run(rx);
            });
        }

        let client = SyncRpcClient::new(Arc::clone(&transport), super_rx);
        Self {
            node_ids,
            client,
            transport,
        }
    }

    fn list_status(&self) -> Vec<node::NodeStatus> {
        let mut statuses: Vec<node::NodeStatus> = Vec::new();
        for id in &self.node_ids {
            match self.client.request(*id, node::Message::ShowStatus) {
                node::Message::Status(status) => statuses.push(status),
                msg => panic!("unexpected response: {:?}", msg),
            };
        }
        statuses
    }

    fn majority_count(&self) -> usize {
        (self.node_ids.len() as f64 / 2.0).ceil() as usize
    }

    fn current_leader_of(&self, statuses: &[node::NodeStatus]) -> Option<u32> {
        let mut counts = vec![0; self.node_ids.len()];
        for st in statuses {
            counts[st.leader as usize - 1] += 1;
        }
        let mut leaders: Vec<_> = counts
            .into_iter()
            .enumerate()
            .filter(|(_, c)| *c as usize >= self.majority_count())
            .collect();
        if leaders.len() != 1 {
            return None;
        }
        let (i, _) = leaders.pop().unwrap();
        Some(i as u32 + 1)
    }

    fn current_leader(&self) -> Option<u32> {
        self.current_leader_of(&self.list_status())
    }
}

#[test]
fn test_election() {
    env_logger::init();

    let ctx = TestingContext::new(NUM_NODES);

    thread::sleep(ELECTION_TIMEOUT_MAX * 2);

    let statuses = ctx.list_status();
    let leader = ctx.current_leader_of(&statuses).unwrap();
    for i in 0..NUM_NODES {
        let is_leader = leader == (i + 1) as u32;
        assert_eq!(
            if is_leader { "leader" } else { "follower" },
            statuses[i].state
        );
        assert_eq!(statuses[0].leader, statuses[i].leader);
        assert_eq!(statuses[0].term, statuses[i].term);
        assert_eq!(statuses[0].value, statuses[i].value);
    }
    let last_term = statuses[0].term;

    let take_down = *ctx.node_ids.iter().find(|x| **x != leader).unwrap();
    ctx.transport.lock().unwrap().down(take_down);
    thread::sleep(ELECTION_TIMEOUT_MAX * 2);

    let statuses = ctx.list_status();
    for i in 0..NUM_NODES {
        if (i as u32 + 1) == take_down {
            continue;
        }

        let is_leader = leader == (i + 1) as u32;
        assert_eq!(
            if is_leader { "leader" } else { "follower" },
            statuses[i].state
        );
        assert_eq!(leader as i32, statuses[i].leader);
        assert_eq!(last_term, statuses[i].term);
        assert_eq!(statuses[0].value, statuses[i].value);
    }
}
