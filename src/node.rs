use log::{debug, info};
use rand::Rng;
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

// must be smaller than election timeout min
const ELECTION_TIMEOUT_MIN: Duration = Duration::from_secs(5);
const ELECTION_TIMEOUT_MAX: Duration = Duration::from_secs(10);
// const ELECTION_TIMEOUT_MIN: Duration = Duration::from_millis(150);
// const ELECTION_TIMEOUT_MAX: Duration = Duration::from_millis(300);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(2);

pub trait Transport {
    fn send(&mut self, from: u32, to: u32, msg: Message);
}

#[derive(Clone, Copy)]
struct Term {
    seq: u64,
    voted: bool,
}

impl Term {
    fn new(seq: u64) -> Self {
        Self { seq, voted: false }
    }
}

pub struct RaftNode<T: Transport> {
    id: u32,
    term: Term,
    transport: Arc<Mutex<T>>,
    nodes: Vec<u32>,
    state: NodeState,
    value: i32,
}

fn next_election_timeout() -> Duration {
    let mut rng = rand::thread_rng();
    let timeout = rng.gen_range(
        ELECTION_TIMEOUT_MIN.as_millis() as u64..ELECTION_TIMEOUT_MAX.as_millis() as u64,
    );
    Duration::from_millis(timeout)
}

pub struct CandidateState {
    timeout_at: Instant,
    vote_count: u32,
}

impl CandidateState {
    pub fn new() -> Self {
        Self {
            timeout_at: Instant::now() + next_election_timeout(),
            vote_count: 0,
        }
    }
}

pub struct FollowerState {
    next_election_at: Instant,
    leader: Option<u32>,
}

impl FollowerState {
    pub fn new(leader: Option<u32>) -> Self {
        Self {
            next_election_at: Instant::now() + next_election_timeout(),
            leader,
        }
    }
}

pub struct LeaderState {
    next_append_at: Instant,
}

impl LeaderState {
    pub fn new() -> Self {
        Self {
            next_append_at: Instant::now() + HEARTBEAT_TIMEOUT,
        }
    }
}

pub enum NodeState {
    Follower(FollowerState),
    Candidate(CandidateState),
    Leader(LeaderState),
}

impl NodeState {
    fn next_timeout(&self) -> Instant {
        use NodeState::*;
        match self {
            Follower(state) => state.next_election_at,
            Candidate(state) => state.timeout_at,
            Leader(state) => state.next_append_at,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct NodeStatus {
    pub state: &'static str,
    pub leader: i32,
    pub term: u64,
    pub election_timeout: Duration,
}

#[derive(Clone, Copy, Debug)]
pub enum Message {
    RequestVote { term: u64 },
    Vote,
    AppendEntries { term: u64 },

    ShowStatus,
    Status(NodeStatus),
}

impl<T: Transport> RaftNode<T> {
    pub fn new(id: u32, nodes: Vec<u32>, transport: Arc<Mutex<T>>) -> Self {
        Self {
            id,
            term: Term::new(0),
            nodes,
            transport,
            state: NodeState::Follower(FollowerState::new(None)),
            value: 0,
        }
    }

    fn state_str(&self) -> &'static str {
        match &self.state {
            NodeState::Follower(_) => "follower",
            NodeState::Candidate(_) => "candidate",
            NodeState::Leader(_) => "leader",
        }
    }

    pub fn set_value(&mut self, value: i32) {
        self.value = value;
    }

    fn broadcast(&mut self, msg: Message) {
        for &address in &self.nodes {
            if address != self.id {
                self.send_message(address, msg);
            }
        }
    }

    fn majority_count(&self) -> u32 {
        let c = (self.nodes.len() as f64 / 2.0).ceil() as u32;
        eprintln!("majority count: {}", c);
        c
    }

    fn start_term(&mut self, term: u64) {
        self.term = Term::new(term);
    }

    fn renew_term(&mut self) {
        self.term = Term::new(self.term.seq + 1);
    }

    fn send_message(&self, to: u32, msg: Message) {
        self.transport.lock().unwrap().send(self.id, to, msg);
    }

    pub fn recv(&mut self, from: u32, msg: Message) {
        match msg {
            Message::RequestVote { term } => {
                if self.term.seq < term {
                    self.start_term(term);
                    debug!(
                        "{}: Starting new term {} by RequestVote from {}",
                        self.lh(),
                        self.term.seq,
                        from
                    );
                }
                if !self.term.voted {
                    debug!(
                        "{}: Voting to {} for term {}",
                        self.lh(),
                        from,
                        self.term.seq
                    );
                    self.send_message(from, Message::Vote);
                    self.become_follower(None)
                }
            }
            Message::Vote => {
                if let NodeState::Candidate(state) = &mut self.state {
                    state.vote_count += 1;
                    if state.vote_count >= self.majority_count() {
                        self.become_leader();
                    }
                }
            }
            Message::AppendEntries { term } => {
                if term < self.term.seq {
                    debug!(
                        "{}: Ignoring AE with term below the current: {} < {}",
                        self.lh(),
                        term,
                        self.term.seq
                    );
                    return;
                }

                if let NodeState::Follower(state) = &mut self.state {
                    if state.leader.is_none() {
                        self.become_follower(Some(from));
                    } else {
                        let timeout = next_election_timeout();
                        state.next_election_at = Instant::now() + timeout;
                    }
                } else if term > self.term.seq {
                    // If now leader or candidate and receive bigger term, we should start following the new leader
                    debug!(
                        "{}: Received bigger term, start following {} as new leader",
                        self.lh(),
                        from
                    );
                    self.start_term(term);
                    self.become_follower(Some(from));
                }
            }
            Message::ShowStatus => {
                let status = NodeStatus {
                    state: self.state_str(),
                    leader: self.current_leader(),
                    term: self.term.seq,
                    election_timeout: match &self.state {
                        NodeState::Follower(st) => st.next_election_at - Instant::now(),
                        NodeState::Candidate(st) => st.timeout_at - Instant::now(),
                        NodeState::Leader(_) => Duration::from_millis(0),
                    },
                };
                self.send_message(from, Message::Status(status));
            }
            Message::Status(_) => {
                // ignore
            }
        }
    }

    fn current_leader(&self) -> i32 {
        match &self.state {
            NodeState::Leader(_) => self.id as i32,
            NodeState::Follower(st) => st.leader.map(|v| v as i32).unwrap_or(-1),
            NodeState::Candidate(_) => -1,
        }
    }

    fn lh(&self) -> String {
        format!("{}[{}]", self.id, self.state_str())
    }

    fn become_candidate(&mut self) {
        self.renew_term();
        debug!(
            "{}: become CANDIDATE with term {}",
            self.lh(),
            self.term.seq
        );
        let mut state = CandidateState::new();
        state.vote_count += 1;
        self.state = NodeState::Candidate(state);
        self.broadcast(Message::RequestVote {
            term: self.term.seq,
        });
    }

    fn become_follower(&mut self, leader: Option<u32>) {
        debug!("{}: become FOLLOWER for leader {:?}", self.lh(), leader);
        let state = FollowerState::new(leader);
        self.state = NodeState::Follower(state);
    }

    fn become_leader(&mut self) {
        debug!("{}: become LEADER", self.lh());
        let state = LeaderState::new();
        self.state = NodeState::Leader(state);
    }

    fn send_heartbeat(&mut self) {
        // self.log_state("Sending heartbeats");
        self.broadcast(Message::AppendEntries {
            term: self.term.seq,
        });
    }

    fn scheduled_work(&mut self) -> Instant {
        match &mut self.state {
            NodeState::Follower(state) => {
                let now = Instant::now();
                if now >= state.next_election_at {
                    self.become_candidate();
                }
            }
            NodeState::Candidate(state) => {
                let now = Instant::now();
                if now >= state.timeout_at {
                    self.become_follower(None);
                }
            }
            NodeState::Leader(ref mut state) => {
                let now = Instant::now();
                if now >= state.next_append_at {
                    state.next_append_at = now + HEARTBEAT_TIMEOUT;
                    self.send_heartbeat();
                }
            }
        }

        self.state.next_timeout()
    }

    pub fn run(&mut self, rx: mpsc::Receiver<(u32, Message)>) -> ! {
        let mut next_timeout = Instant::now();
        loop {
            let now = Instant::now();
            let timeout = if next_timeout > now {
                next_timeout - now
            } else {
                Duration::from_secs(0)
            };
            match rx.recv_timeout(timeout) {
                Ok((from, msg)) => self.recv(from, msg),
                Err(e) => match e {
                    mpsc::RecvTimeoutError::Timeout => {}
                    mpsc::RecvTimeoutError::Disconnected => {
                        eprintln!("error receiving message from mpsc: {:?}", e);
                    }
                },
            }
            next_timeout = self.scheduled_work();
        }
    }
}
