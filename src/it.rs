use crate::node::{self, Transport};
use crate::transport::{self, InMemoryTransport};
use env_logger;
use std::sync::Once;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

const NUM_NODES: usize = 3;
const ELECTION_TIMEOUT_MIN: Duration = Duration::from_millis(150);
const ELECTION_TIMEOUT_MAX: Duration = Duration::from_millis(300);

const MAX_LOG_SIZE: usize = 5;

static INIT: Once = Once::new();

fn setup() {
    INIT.call_once(|| {
        env_logger::init();
    });
}

struct SyncRpcClient<T: Transport> {
    transport: Arc<Mutex<T>>,
    rx: mpsc::Receiver<(u32, node::Message)>,
}

impl<T: Transport> SyncRpcClient<T> {
    fn new(transport: Arc<Mutex<T>>, rx: mpsc::Receiver<(u32, node::Message)>) -> Self {
        Self { transport, rx }
    }

    fn request(&self, to: u32, msg: node::Message) -> node::Message {
        self.send(to, msg);
        let (from, msg) = self.rx.recv().unwrap();
        if from != to {
            panic!("unexpected response from {}", from);
        }
        msg
    }

    fn send(&self, to: u32, msg: node::Message) {
        self.transport.lock().unwrap().send(0, to, msg);
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

        let client = SyncRpcClient::new(Arc::clone(&transport), super_rx);
        let mut this = Self {
            node_ids,
            client,
            transport,
        };

        for i in 1..=num_nodes {
            this.add_node(i as u32);
        }

        this
    }

    fn add_node(&mut self, id: u32) {
        let (tx, rx) = mpsc::channel();
        self.transport.lock().unwrap().add_node(tx);
        let tp = Arc::clone(&self.transport);
        let ids = self.node_ids.clone();
        thread::spawn(move || {
            let mut node = node::RaftNode::new(id, ids, tp);
            node.set_election_timeout(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX);
            node.set_max_log_size(MAX_LOG_SIZE);
            node.run(rx);
        });
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
            if st.leader == -1 {
                continue;
            }
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

    fn current_values_of(&self, statuses: &[node::NodeStatus]) -> Vec<i32> {
        statuses.into_iter().map(|st| st.value).collect()
    }

    fn current_values(&self) -> Vec<i32> {
        self.current_values_of(&self.list_status())
    }

    fn leader_value(&self) -> i32 {
        let statuses = self.list_status();
        let leader = self.current_leader_of(&statuses).unwrap();
        let values = self.current_values_of(&statuses);
        values[leader as usize - 1]
    }
}

#[test]
fn test_election() {
    setup();

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
    }

    ctx.transport.lock().unwrap().down(leader);
    thread::sleep(ELECTION_TIMEOUT_MAX * 2);

    let statuses = ctx.list_status();
    for i in 0..NUM_NODES {
        let id = i as u32 + 1;
        if id == take_down || id == leader {
            continue;
        }

        assert_ne!("leader", statuses[i].state);
        assert_eq!(-1, statuses[i].leader);
    }

    ctx.transport.lock().unwrap().up(take_down);
    thread::sleep(ELECTION_TIMEOUT_MAX * 2);

    let statuses = ctx.list_status();
    let new_leader = ctx.current_leader_of(&statuses).unwrap();
    for i in 0..NUM_NODES {
        let id = i as u32 + 1;
        if id == leader {
            continue;
        }

        let is_leader = new_leader == (i + 1) as u32;
        assert_eq!(
            if is_leader { "leader" } else { "follower" },
            statuses[i].state
        );
        assert_eq!(new_leader as i32, statuses[i].leader);
        assert!(statuses[i].term > last_term);
    }

    ctx.transport.lock().unwrap().up(leader);
    thread::sleep(ELECTION_TIMEOUT_MAX * 2);

    let statuses = ctx.list_status();
    let new_leader = ctx.current_leader_of(&statuses).unwrap();
    for i in 0..NUM_NODES {
        let is_leader = new_leader == (i + 1) as u32;
        assert_eq!(
            if is_leader { "leader" } else { "follower" },
            statuses[i].state
        );
        assert_eq!(new_leader as i32, statuses[i].leader);
        assert!(statuses[i].term > last_term);
    }
}

#[test]
fn test_replication() {
    setup();

    let ctx = TestingContext::new(NUM_NODES);

    thread::sleep(ELECTION_TIMEOUT_MAX * 2);

    // TODO: should try randomly and get redirected?
    let leader = ctx.current_leader().unwrap();
    if let node::Message::UpdateValueResult(err) = ctx.client.request(
        leader,
        node::Message::UpdateValue(node::ops::Operation::Set(10)),
    ) {
        assert_eq!(None, err);
    }

    assert_eq!(10, ctx.leader_value());

    if let node::Message::UpdateValueResult(err) = ctx.client.request(
        leader,
        node::Message::UpdateValue(node::ops::Operation::Set(20)),
    ) {
        assert_eq!(None, err);
    }
    assert_eq!(20, ctx.leader_value());

    let mut value = 20;
    for _ in 0..ctx.node_ids.len() {
        let leader = ctx.current_leader().unwrap();
        thread::sleep(ELECTION_TIMEOUT_MAX);
        ctx.transport.lock().unwrap().down(leader);
        thread::sleep(ELECTION_TIMEOUT_MAX * 2);
        assert_eq!(value, ctx.leader_value());

        let new_leader = ctx.current_leader().unwrap();
        value += 10;
        if let node::Message::UpdateValueResult(err) = ctx.client.request(
            new_leader,
            node::Message::UpdateValue(node::ops::Operation::Set(value)),
        ) {
            assert_eq!(None, err);
        }
        assert_eq!(value, ctx.leader_value());

        ctx.transport.lock().unwrap().up(leader);
    }
}

#[test]
fn test_membership_change() {
    setup();

    let mut ctx = TestingContext::new(NUM_NODES);
    thread::sleep(ELECTION_TIMEOUT_MAX * 2);

    let value = 10;
    let leader = ctx.current_leader().unwrap();
    if let node::Message::UpdateValueResult(err) = ctx.client.request(
        leader,
        node::Message::UpdateValue(node::ops::Operation::Set(value)),
    ) {
        assert_eq!(None, err);
    }

    let added_nodes = vec![NUM_NODES as u32 + 1, NUM_NODES as u32 + 2];
    ctx.node_ids.extend(&added_nodes);
    for &id in &added_nodes {
        ctx.add_node(id);
    }
    ctx.client
        .send(leader, node::Message::UpdateConfig(ctx.node_ids.clone()));

    thread::sleep(ELECTION_TIMEOUT_MAX * 2);
    ctx.transport.lock().unwrap().down(leader);
    thread::sleep(ELECTION_TIMEOUT_MAX * 2);

    // TODO: check for split brain?
    let statuses = ctx.list_status();
    let new_leader = ctx.current_leader_of(&statuses).unwrap();
    for i in 0..ctx.node_ids.len() {
        if i as u32 + 1 == leader {
            continue;
        }

        let id = i as u32 + 1;
        let is_leader = new_leader == id;
        assert_eq!(
            if is_leader { "leader" } else { "follower" },
            statuses[i].state
        );
        assert_eq!(new_leader as i32, statuses[i].leader);
        assert_eq!(statuses[new_leader as usize - 1].term, statuses[i].term);
        if id != leader {
            assert_eq!(value, statuses[i].value);
        }
    }

    let value = 20;
    if let node::Message::UpdateValueResult(err) = ctx.client.request(
        new_leader,
        node::Message::UpdateValue(node::ops::Operation::Set(value)),
    ) {
        assert_eq!(None, err);
    }
    ctx.transport.lock().unwrap().up(leader);
    thread::sleep(ELECTION_TIMEOUT_MAX * 2);

    let statuses = ctx.list_status();
    let latest_leader = ctx.current_leader_of(&statuses).unwrap();
    assert_eq!(value, ctx.leader_value());
    for i in 0..ctx.node_ids.len() {
        let id = i as u32 + 1;
        let is_leader = latest_leader == id;
        assert_eq!(
            if is_leader { "leader" } else { "follower" },
            statuses[i].state
        );
        assert_eq!(latest_leader as i32, statuses[i].leader);
        assert_eq!(statuses[new_leader as usize - 1].term, statuses[i].term);
        assert_eq!(value, statuses[i].value);
    }
}

#[test]
fn test_membership_change_migration() {
    setup();

    let mut ctx = TestingContext::new(NUM_NODES);
    thread::sleep(ELECTION_TIMEOUT_MAX * 2);

    let value = 10;
    let leader = ctx.current_leader().unwrap();
    if let node::Message::UpdateValueResult(err) = ctx.client.request(
        leader,
        node::Message::UpdateValue(node::ops::Operation::Set(value)),
    ) {
        assert_eq!(None, err);
    }

    let replace_nodes: Vec<_> = (1..=NUM_NODES).map(|i| (NUM_NODES + i) as u32).collect();
    ctx.node_ids.extend(&replace_nodes);
    for &id in &replace_nodes {
        ctx.add_node(id);
    }
    ctx.client
        .send(leader, node::Message::UpdateConfig(replace_nodes.clone()));

    thread::sleep(ELECTION_TIMEOUT_MAX * 30);

    let statuses = ctx.list_status();
    let new_leader = ctx.current_leader_of(&statuses[NUM_NODES..]).unwrap();

    // New leader must have elected from nodes in replacing group.
    assert!(replace_nodes.contains(&new_leader));

    for st in &statuses[NUM_NODES..] {
        let is_leader = new_leader == st.id;
        assert_eq!(if is_leader { "leader" } else { "follower" }, st.state);
        assert_eq!(new_leader as i32, st.leader);
        assert_eq!(statuses[new_leader as usize - 1].term, st.term);
        assert_eq!(value, st.value);
    }

    for st in &statuses[..NUM_NODES] {
        // All old nodes including ex-leader must have reverted its state to follower (or candidate).
        assert_ne!("leader", st.state);
    }

    for id in 1..=NUM_NODES {
        // Take down all old nodes
        ctx.transport.lock().unwrap().down(id as u32);
    }

    let value = 20;
    if let node::Message::UpdateValueResult(err) = ctx.client.request(
        new_leader,
        node::Message::UpdateValue(node::ops::Operation::Set(value)),
    ) {
        assert_eq!(None, err);
    }
    thread::sleep(ELECTION_TIMEOUT_MAX * 2);

    let statuses = ctx.list_status();
    for st in &statuses[NUM_NODES..] {
        assert_eq!(value, st.value);
    }
}

#[test]
fn test_snapshot() {
    setup();

    let ctx = TestingContext::new(NUM_NODES);
    thread::sleep(ELECTION_TIMEOUT_MAX * 2);

    let leader = ctx.current_leader().unwrap();
    let take_down = *ctx.node_ids.iter().find(|x| **x != leader).unwrap();
    ctx.transport.lock().unwrap().down(take_down);
    thread::sleep(ELECTION_TIMEOUT_MAX * 2);

    let mut value = 10;
    for _ in 0..(MAX_LOG_SIZE * 2) {
        value += 1;
        if let node::Message::UpdateValueResult(err) = ctx.client.request(
            leader,
            node::Message::UpdateValue(node::ops::Operation::Set(value)),
        ) {
            assert_eq!(None, err);
        }
    }

    thread::sleep(ELECTION_TIMEOUT_MAX * 2);

    ctx.transport.lock().unwrap().up(take_down);
    thread::sleep(ELECTION_TIMEOUT_MAX * 4);

    let statuses = ctx.list_status();
    for st in &statuses {
        assert_eq!(value, st.value);
        assert!(st.log_size <= MAX_LOG_SIZE);
    }
}
