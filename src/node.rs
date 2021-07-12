use crate::log::{self as rlog, EntryId};
use log::{debug, info};
use rand::Rng;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::rc::Rc;
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

// must be smaller than election timeout min
pub const ELECTION_TIMEOUT_MIN: Duration = Duration::from_secs(5);
pub const ELECTION_TIMEOUT_MAX: Duration = Duration::from_secs(10);
// const ELECTION_TIMEOUT_MIN: Duration = Duration::from_millis(150);
// const ELECTION_TIMEOUT_MAX: Duration = Duration::from_millis(300);
pub const HEARTBEAT_TIMEOUT: Duration = Duration::from_millis(50);

const REPLICATION_CAUGHT_UP_DIFF: isize = 10;

pub trait Transport {
    fn send(&mut self, from: u32, to: u32, msg: Message);
}

#[derive(Clone, Copy)]
struct Term {
    seq: u64,
    voted_for: Option<u32>,
}

impl Term {
    fn new(seq: u64) -> Self {
        Self {
            seq,
            voted_for: None,
        }
    }
}

pub type ClientAddress = u32;

struct ReplicaStates {
    states: HashMap<u32, Rc<RefCell<ReplicaState>>>,
}

impl ReplicaStates {
    fn new(ids: &[u32], log: &rlog::Log, current: Option<&ReplicaStates>) -> Self {
        let mut states = HashMap::new();
        for &id in ids {
            if let Some(cur) = current {
                if let Some(r) = cur.get(id) {
                    states.insert(id, Rc::clone(r));
                    continue;
                }
            }
            states.insert(id, Rc::new(RefCell::new(ReplicaState::new(log))));
        }
        Self { states }
    }

    fn get(&self, id: u32) -> Option<&Rc<RefCell<ReplicaState>>> {
        self.states.get(&id)
    }

    fn index_high_watermark(&self) -> usize {
        let mut indexes: Vec<_> = self
            .states
            .values()
            .map(|s| s.borrow().match_index)
            .collect();
        indexes.sort();
        indexes[indexes.len() - self.majority_count()]
    }

    fn majority_count(&self) -> usize {
        majority_count_of(self.states.len())
    }
}

struct ReplicaStatesGroup {
    current_replicas: ReplicaStates,
    new_replicas: Option<ReplicaStates>,
}

impl ReplicaStatesGroup {
    fn new(config: &ClusterConfig, log: &rlog::Log) -> Self {
        let current = ReplicaStates::new(config.current_nodes.ids(), log, None);
        let new_replicas = Self::new_replicas_value(config, log, &current);
        Self {
            current_replicas: current,
            new_replicas,
        }
    }

    fn new_replicas_value(
        config: &ClusterConfig,
        log: &rlog::Log,
        current: &ReplicaStates,
    ) -> Option<ReplicaStates> {
        config
            .new_nodes
            .as_ref()
            .map(|nodes| ReplicaStates::new(nodes.ids(), log, Some(current)))
    }

    fn index_high_watermark(&self, joint_consensus: bool) -> usize {
        let mut hw = self.current_replicas.index_high_watermark();
        if joint_consensus {
            if let Some(replicas) = &self.new_replicas {
                hw = hw.min(replicas.index_high_watermark());
            }
        }
        hw
    }

    fn get(&self, id: u32) -> Option<&Rc<RefCell<ReplicaState>>> {
        self.current_replicas
            .get(id)
            .or_else(|| self.new_replicas.as_ref().and_then(|r| r.get(id)))
    }

    fn set_new_replicas(&mut self, config: &ClusterConfig, log: &rlog::Log) {
        self.new_replicas = Self::new_replicas_value(config, log, &self.current_replicas);
    }

    fn promote(&mut self) -> Self {
        Self {
            current_replicas: self.new_replicas.take().unwrap(),
            new_replicas: None,
        }
    }
}

pub struct LeaderState {
    next_append_at: Instant,
    replica_states: ReplicaStatesGroup,
    pending_requests: VecDeque<(usize, ClientAddress)>,
}

impl LeaderState {
    pub fn new(config: &ClusterConfig, log: &rlog::Log) -> Self {
        Self {
            next_append_at: Instant::now() + HEARTBEAT_TIMEOUT,
            replica_states: ReplicaStatesGroup::new(config, log),
            pending_requests: VecDeque::new(),
        }
    }

    fn add_pending_request(&mut self, addr: u32, index: usize) {
        self.pending_requests.push_back((index, addr));
    }

    fn client_responses(&mut self, commit_index: usize) -> Vec<(usize, u32)> {
        let mut targets = Vec::new();
        while let Some((index, _)) = self.pending_requests.front() {
            if *index > commit_index {
                break;
            }
            targets.push(self.pending_requests.pop_front().unwrap());
        }
        targets
    }
}

#[derive(Debug, Clone)]
struct ReplicaState {
    next_index: usize, // leader's last log index + 1
    match_index: usize,
}

impl ReplicaState {
    fn new(log: &rlog::Log) -> Self {
        Self {
            next_index: log.last_index() + 1,
            match_index: 0,
        }
    }
}

fn majority_count_of(num_nodes: usize) -> usize {
    (num_nodes as f64 / 2.0).ceil() as usize
}

#[derive(Debug, Clone)]
pub struct Nodes {
    ids: Vec<u32>,
}

impl Nodes {
    pub fn new(ids: Vec<u32>) -> Self {
        Self { ids }
    }

    fn majority_count(&self) -> usize {
        majority_count_of(self.ids.len())
    }

    fn len(&self) -> usize {
        self.ids.len()
    }

    fn ids(&self) -> &[u32] {
        &self.ids
    }
}

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    log_index: usize,
    current_nodes: Nodes,
    new_nodes: Option<Nodes>,
    new_nodes_observer: bool,
}

impl ClusterConfig {
    pub fn new(
        log_index: usize,
        nodes: Nodes,
        new_nodes: Option<Nodes>,
        new_nodes_observer: bool,
    ) -> Self {
        Self {
            log_index,
            current_nodes: nodes,
            new_nodes: new_nodes,
            new_nodes_observer,
        }
    }

    fn all_nodes(&self) -> Vec<u32> {
        let mut seen = HashSet::new();
        for &id in self.current_nodes.ids() {
            seen.insert(id);
        }
        if let Some(new) = &self.new_nodes {
            for &id in new.ids() {
                seen.insert(id);
            }
        }
        seen.into_iter().collect()
    }

    fn is_voting_member(&self, id: u32) -> bool {
        self.current_nodes.ids().contains(&id)
            || self.new_nodes_observer
                && self
                    .new_nodes
                    .as_ref()
                    .map(|nodes| nodes.ids().contains(&id))
                    .unwrap_or(false)
    }
}

pub struct RaftNode<T: Transport> {
    id: u32,
    term: Term,
    transport: Arc<Mutex<T>>,
    config: ClusterConfig,
    state: NodeState,
    value: i32,
    log: rlog::Log,
    election_timeout_min: Duration,
    election_timeout_max: Duration,
}

pub struct CandidateState {
    timeout_at: Instant,
    vote_count: usize,
    new_vote_count: usize,
}

impl CandidateState {
    pub fn new(election_timeout: Duration) -> Self {
        Self {
            timeout_at: Instant::now() + election_timeout,
            vote_count: 0,
            new_vote_count: 0,
        }
    }
}

pub struct FollowerState {
    next_election_at: Instant,
    last_heartbeat_at: Option<Instant>,
    leader: Option<u32>,
}

impl FollowerState {
    pub fn new(leader: Option<u32>, election_timeout: Duration) -> Self {
        Self {
            next_election_at: Instant::now() + election_timeout,
            last_heartbeat_at: None,
            leader,
        }
    }

    fn extend_election_timeout(&mut self, from: u32, timeout: Duration) {
        // TODO: should check current leader?
        if self.leader.is_none() {
            self.leader = Some(from);
        }

        let now = Instant::now();
        self.next_election_at = now + timeout;
        self.last_heartbeat_at = Some(now);
    }

    fn last_heartbeat_within(&self, duration: Duration) -> bool {
        self.last_heartbeat_at
            .map(|at| Instant::now() - at <= duration)
            .unwrap_or(false)
    }
}

pub enum NodeState {
    Follower(FollowerState),
    Candidate(CandidateState),
    Leader(LeaderState),
}

impl fmt::Display for NodeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use NodeState::*;
        write!(
            f,
            "{}",
            match self {
                Follower(_) => "follower",
                Candidate(_) => "candidate",
                Leader(_) => "leader",
            }
        )
    }
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

#[derive(Clone, Debug)]
pub struct NodeStatus {
    pub id: u32,
    pub state: String,
    pub leader: i32,
    pub term: u64,
    pub election_timeout: Duration,
    pub value: i32,
    pub config: ClusterConfig,
}

#[derive(Clone, Debug)]
pub enum Message {
    RequestVote {
        term: u64,
        last_log_index: usize,
        last_log_term: u64,
    },
    RequestVoteResponse {
        term: u64,
        vote_granted: bool,
    },
    AppendEntries {
        term: u64,
        leader_id: u32,
        prev_log_index: usize,
        prev_log_term: u64,
        entries: Vec<rlog::LogEntry>,
        leader_commit: usize,
    },
    AppendEntriesResponse {
        term: u64,
        success: bool,
        last_append_index: usize,
    },

    UpdateValue(ops::Operation),
    UpdateValueResult(Option<&'static str>),
    UpdateConfig(Vec<u32>),

    ShowStatus,
    Status(NodeStatus),
}

impl<T: Transport> RaftNode<T> {
    pub fn new(id: u32, nodes: Vec<u32>, transport: Arc<Mutex<T>>) -> Self {
        let mut this = Self {
            id,
            term: Term::new(0),
            config: ClusterConfig::new(0, Nodes::new(nodes), None, false),
            transport,
            state: NodeState::Follower(FollowerState::new(None, Duration::from_secs(0))),
            value: 0,
            log: rlog::Log::new(),
            election_timeout_min: ELECTION_TIMEOUT_MIN,
            election_timeout_max: ELECTION_TIMEOUT_MAX,
        };
        this.become_follower(None);
        this
    }

    pub fn set_election_timeout(&mut self, min: Duration, max: Duration) {
        self.election_timeout_min = min;
        self.election_timeout_max = max;
        self.become_follower(None);
    }

    fn next_election_timeout(&self) -> Duration {
        let mut rng = rand::thread_rng();
        let timeout = rng.gen_range(
            self.election_timeout_min.as_millis() as u64
                ..self.election_timeout_max.as_millis() as u64,
        );
        Duration::from_millis(timeout)
    }

    fn send_message(&self, to: u32, msg: Message) {
        self.transport.lock().unwrap().send(self.id, to, msg);
    }

    fn send_vote_rejection(&self, from: u32) {
        self.send_message(
            from,
            Message::RequestVoteResponse {
                term: self.term.seq,
                vote_granted: false,
            },
        );
    }

    fn handle_request_vote(
        &mut self,
        from: u32,
        term: u64,
        last_log_index: usize,
        last_log_term: u64,
    ) {
        if !self.config.is_voting_member(from) {
            debug!(
                "{}: Not voting for {} because it is not an active member",
                self.lh(),
                from
            );
            self.send_vote_rejection(from);
            return;
        }

        if let NodeState::Follower(state) = &self.state {
            if state.last_heartbeat_within(self.election_timeout_min) {
                debug!(
                    "{}: Not voting for {} because {:?} has not elapsed since last heartbeat",
                    self.lh(),
                    from,
                    self.election_timeout_min
                );
                self.send_vote_rejection(from);
                return;
            }
        }
        if term < self.term.seq {
            // Do not vote if candidate's term is below mine.
            debug!(
                "{}: Not voting for {} because it has lower term than mine: {} < {}",
                self.lh(),
                from,
                term,
                self.term.seq,
            );
            self.send_vote_rejection(from);
            return;
        }
        if self.term.seq < term {
            // If this candidate has higher term, we should start a new term before any further checks.
            self.start_term(term);
            debug!(
                "{}: Starting new term and becoming follower {} by RequestVote from {}",
                self.lh(),
                self.term.seq,
                from
            );
            self.become_follower(None);
        }

        let last_entry_id = EntryId {
            index: last_log_index,
            term: last_log_term,
        };
        // 5.4.1 Election restriction
        if last_entry_id < self.log.last_entry_id() {
            // Do not vote if candidate's term is below mine.
            debug!(
                "{}: Not voting for {} because it has lower last log ID than mine: {:?} < {:?}",
                self.lh(),
                from,
                last_entry_id,
                self.log.last_entry_id(),
            );
            self.send_vote_rejection(from);
            return;
        }

        if let Some(voted_for) = self.term.voted_for {
            if voted_for != from {
                // Do not vote if I've already voted for an another candidate
                self.send_vote_rejection(from);
                return;
            }
        }

        self.term.voted_for = Some(from);
        self.send_message(
            from,
            Message::RequestVoteResponse {
                term: self.term.seq,
                vote_granted: true,
            },
        );
        self.become_follower(None);
    }

    fn send_ae_rejection(&mut self, from: u32) {
        self.send_message(
            from,
            Message::AppendEntriesResponse {
                term: self.term.seq,
                success: false,
                last_append_index: 0,
            },
        );
    }

    fn handle_append_entries(
        &mut self,
        from: u32,
        term: u64,
        leader_id: u32,
        prev_log_index: usize,
        prev_log_term: u64,
        entries: Vec<rlog::LogEntry>,
        leader_commit: usize,
    ) {
        if term < self.term.seq {
            debug!(
                "{}: Ignoring AE with term below the current: {} < {}",
                self.lh(),
                term,
                self.term.seq
            );
            self.send_ae_rejection(from);
            return;
        }

        // debug!("{}: AppendEntries from {}", self.lh(), leader_id);

        let election_to = self.next_election_timeout();
        if let NodeState::Follower(state) = &mut self.state {
            state.extend_election_timeout(from, election_to);
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

        if prev_log_index > 0 {
            if let Some(entry) = self.log.get(prev_log_index) {
                if entry.term != prev_log_term {
                    // Conflict
                    self.log.truncate_to(prev_log_index);
                }
            } else {
                debug!(
                    "{}: Log entry {} does not exists, fail to process AppendEntries from {}",
                    self.lh(),
                    prev_log_index,
                    from
                );
                self.send_ae_rejection(from);
                return;
            }
        }

        for (i, entry) in entries.into_iter().enumerate() {
            let index = prev_log_index + i + 1;
            if index <= self.log.last_index() {
                // Dedup. 5.5 Follower and candidate crashes
                continue;
            }

            if let Some(new_config) = &entry.config_update {
                debug!(
                    "{}: in follower config becomes: {:?}",
                    self.lh(),
                    new_config
                );
                self.config = new_config.clone();
            }

            debug!("{}: appending entry in follower: {:?}", self.lh(), entry);
            self.log.append(entry);
        }

        if leader_commit > self.log.committed_index() {
            debug!("{}: Committing {}", self.lh(), leader_commit);
            self.value = self.log.commit(leader_commit, self.value);
        }

        self.send_message(
            from,
            Message::AppendEntriesResponse {
                term: self.term.seq,
                success: true,
                last_append_index: self.log.last_index(),
            },
        );
    }

    fn broadcast(&mut self, msg: Message) {
        for address in self.config.all_nodes() {
            if address != self.id {
                self.send_message(address, msg.clone());
            }
        }
    }

    fn start_term(&mut self, term: u64) {
        self.term = Term::new(term);
    }

    fn renew_term(&mut self) {
        self.term = Term::new(self.term.seq + 1);
    }

    pub fn recv(&mut self, from: u32, msg: Message) {
        match msg {
            Message::RequestVote {
                term,
                last_log_index,
                last_log_term,
            } => {
                self.handle_request_vote(from, term, last_log_index, last_log_term);
            }
            Message::RequestVoteResponse { term, vote_granted } => {
                if term != self.term.seq {
                    return;
                }
                if !vote_granted {
                    return;
                }

                if let NodeState::Candidate(state) = &mut self.state {
                    let mut got_majority = false;
                    if self.config.current_nodes.ids().contains(&from) {
                        state.vote_count += 1;
                        if state.vote_count >= self.config.current_nodes.majority_count() {
                            got_majority = true;
                        }
                    }
                    if let Some(new) = &self.config.new_nodes {
                        if new.ids().contains(&from) {
                            state.new_vote_count += 1;
                        }
                        // "The first issue" of cluster membership change.
                        // Consider new nodes as majority only if they're voting members.
                        got_majority &= self.config.new_nodes_observer
                            || state.new_vote_count >= new.majority_count();
                    }

                    if got_majority {
                        self.become_leader();
                    }
                }
            }
            Message::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => {
                self.handle_append_entries(
                    from,
                    term,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                );
            }
            Message::AppendEntriesResponse {
                term,
                success,
                last_append_index,
            } => {
                let last_commit_index = self.log.committed_index();

                // TODO: should check term also?
                let lh = self.lh();
                if success {
                    if let NodeState::Leader(state) = &mut self.state {
                        let replica_state = state.replica_states.get(from);
                        if replica_state.is_none() {
                            return;
                        }
                        let mut replica_state = replica_state.unwrap().borrow_mut();

                        if replica_state.next_index != last_append_index + 1 {
                            debug!(
                            "{}: Updating replica state [{}], next_index = {}, match_index = {}",
                            lh,
                            from,
                            last_append_index + 1,
                            last_append_index
                        );
                        }
                        replica_state.next_index = last_append_index + 1;
                        replica_state.match_index = last_append_index;
                        drop(replica_state);

                        let hw = state
                            .replica_states
                            .index_high_watermark(!self.config.new_nodes_observer);
                        if hw > self.log.committed_index() {
                            debug!("{}: Committing {} in leader", lh, hw);
                            self.value = self.log.commit(hw, self.value);
                        }
                        let responses = state.client_responses(hw);

                        // "The first issue" of cluster membership change.
                        // Check if observer nodes are ready to be promoted to voting members.
                        if self.config.new_nodes_observer {
                            if let Some(new) = &state.replica_states.new_replicas {
                                let diff =
                                    (state.replica_states.current_replicas.index_high_watermark()
                                        as isize
                                        - new.index_high_watermark() as isize)
                                        .abs();
                                if diff <= REPLICATION_CAUGHT_UP_DIFF {
                                    debug!(
                                        "{}, Promoting observing NEW nodes into voting members, index = {}",
                                        self.lh(),
                                        self.log.last_index() + 1,
                                    );
                                    self.config = ClusterConfig::new(
                                        self.log.last_index() + 1,
                                        self.config.current_nodes.clone(),
                                        self.config.new_nodes.take(),
                                        false,
                                    );
                                    self.append_log(
                                        from,
                                        ops::Operation::Noop,
                                        Some(self.config.clone()),
                                    );
                                }
                            }
                        }

                        for (_index, addr) in responses {
                            self.send_message(addr, Message::UpdateValueResult(None));
                        }
                    }

                    if last_commit_index < self.config.log_index
                        && self.log.committed_index() >= self.config.log_index
                    {
                        // Commit this config update
                        // TODO: should call a method
                        if let Some(new_nodes) = &self.config.new_nodes {
                            // "The first issue" of cluster membership change.
                            // Added nodes are initially non-voting members.
                            if !self.config.new_nodes_observer {
                                self.config = ClusterConfig::new(
                                    self.log.last_index() + 1,
                                    new_nodes.clone(),
                                    None,
                                    false,
                                );
                                debug!(
                                    "{}: in leader config becomes: {:?}",
                                    self.lh(),
                                    self.config
                                );
                                if let NodeState::Leader(state) = &mut self.state {
                                    state.replica_states = state.replica_states.promote();
                                }
                                self.append_log(
                                    from,
                                    ops::Operation::Noop,
                                    Some(self.config.clone()),
                                );
                            }
                        } else {
                            debug!("{}: Update config done", self.lh());
                            // "The second issue" of cluster membership change.
                            // The current leader should step down from leader role if its ID
                            // isn't included in latest nodes list.
                            if !self.config.is_voting_member(self.id) {
                                debug!("{}: Stepping down from leader", self.lh());
                                self.become_follower(None);
                            }
                        }
                    }
                } else {
                    if term > self.term.seq {
                        self.start_term(term);
                        self.become_follower(None);
                    } else {
                        // If the follower rejected because of absent prev_log_index, decrement next_index
                        // and retry.
                        // 5.3 Log replication
                        if let NodeState::Leader(state) = &mut self.state {
                            if let Some(replica_state) = state.replica_states.get(from) {
                                replica_state.borrow_mut().next_index -= 1;
                            }
                        }
                        if let NodeState::Leader(state) = &self.state {
                            self.send_heartbeat(from, state);
                        }
                    }
                }
            }
            Message::ShowStatus => {
                let status = NodeStatus {
                    id: self.id,
                    state: self.state.to_string(),
                    leader: self.current_leader(),
                    term: self.term.seq,
                    election_timeout: match &self.state {
                        NodeState::Follower(st) => st
                            .next_election_at
                            .checked_duration_since(Instant::now())
                            .unwrap_or_default(),
                        NodeState::Candidate(st) => st
                            .timeout_at
                            .checked_duration_since(Instant::now())
                            .unwrap_or_default(),
                        NodeState::Leader(_) => Duration::from_millis(0),
                    },
                    value: self.value,
                    config: self.config.clone(),
                };
                self.send_message(from, Message::Status(status));
            }
            Message::Status(_) => {
                // ignore
            }
            Message::UpdateValue(op) => {
                self.append_log(from, op, None);
            }
            Message::UpdateValueResult(_) => {
                // ignore
            }
            Message::UpdateConfig(new_nodes) => {
                self.config = ClusterConfig::new(
                    self.log.last_index() + 1,
                    self.config.current_nodes.clone(),
                    Some(Nodes::new(new_nodes)),
                    true,
                );
                self.append_log(from, ops::Operation::Noop, Some(self.config.clone()));
                // TODO: check if on-going migration exists?
                if let NodeState::Leader(state) = &mut self.state {
                    state
                        .replica_states
                        .set_new_replicas(&self.config, &self.log);
                }
            }
        }
    }

    fn append_log(&mut self, from: u32, op: ops::Operation, config_update: Option<ClusterConfig>) {
        let is_config_update = config_update.is_some();
        if let NodeState::Leader(state) = &mut self.state {
            let index = self.log.append(rlog::LogEntry {
                term: self.term.seq,
                op,
                config_update,
            });
            if let Some(replica_state) = state.replica_states.get(self.id) {
                replica_state.borrow_mut().next_index = self.log.last_index() + 1;
                replica_state.borrow_mut().match_index = self.log.last_index();
            }

            if !is_config_update {
                state.add_pending_request(from, index);
            }
        } else if !is_config_update {
            self.send_message(from, Message::UpdateValueResult(Some("not the leader")))
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
        format!("{}[{}]", self.id, self.state.to_string())
    }

    fn become_candidate(&mut self) {
        self.renew_term();
        debug!(
            "{}: become CANDIDATE with term {}",
            self.lh(),
            self.term.seq
        );
        let mut state = CandidateState::new(self.next_election_timeout());
        state.vote_count += 1;
        if let Some(new) = &self.config.new_nodes {
            if new.ids().contains(&self.id) {
                state.new_vote_count += 1;
            }
        }
        self.state = NodeState::Candidate(state);
        let last = self.log.last_entry_id();
        self.broadcast(Message::RequestVote {
            term: self.term.seq,
            last_log_index: last.index,
            last_log_term: last.term,
        });
    }

    fn become_follower(&mut self, leader: Option<u32>) {
        debug!("{}: become FOLLOWER for leader {:?}", self.lh(), leader);
        let state = FollowerState::new(leader, self.next_election_timeout());
        self.state = NodeState::Follower(state);
    }

    fn become_leader(&mut self) {
        debug!("{}: become LEADER", self.lh());
        let state = LeaderState::new(&self.config, &self.log);
        self.state = NodeState::Leader(state);
    }

    fn send_heartbeat(&self, id: u32, state: &LeaderState) {
        // TODO: why just .unwrap() works here?
        if let Some(replica_state) = state.replica_states.get(id) {
            let entries = self
                .log
                .entries_from(replica_state.borrow().next_index)
                .to_vec();

            let prev = self.log.entry_id_at(replica_state.borrow().next_index - 1);
            let msg = Message::AppendEntries {
                term: self.term.seq,
                leader_id: self.id,
                prev_log_index: prev.index,
                prev_log_term: prev.term,
                entries,
                leader_commit: self.log.committed_index(),
            };
            self.send_message(id, msg);
        }
    }

    fn send_heartbeats(&mut self) {
        if let NodeState::Leader(state) = &self.state {
            for id in self.config.all_nodes() {
                if id == self.id {
                    continue;
                }
                self.send_heartbeat(id, state);
            }
        }
        if let NodeState::Leader(state) = &mut self.state {
            state.next_append_at = Instant::now() + HEARTBEAT_TIMEOUT;
        }
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
                    self.send_heartbeats();
                }
            }
        }

        self.state.next_timeout()
    }

    pub fn run(&mut self, rx: mpsc::Receiver<(u32, Message)>) -> ! {
        let mut next_timeout = Instant::now();
        loop {
            let now = Instant::now();
            let timeout = next_timeout.checked_duration_since(now).unwrap_or_default();
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

pub mod ops {
    #[derive(Clone, Copy, Debug)]
    pub enum Operation {
        Noop,
        Set(i32),
        Increment(i32),
    }

    impl Operation {
        pub fn apply(&self, value: i32) -> i32 {
            match *self {
                Operation::Noop => value,
                Operation::Set(val) => val,
                Operation::Increment(to_add) => value + to_add,
            }
        }
    }
}
