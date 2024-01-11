use std::cmp::min;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use scupt_fuzzy::{event_add, fuzzy_enable, fuzzy_message
};

use scupt_net::message_receiver::ReceiverRR;
use scupt_net::message_sender::{Sender, SenderResp, SenderRR};
use scupt_net::notifier::Notifier;

use scupt_util::message::{Message, MsgTrait};
use scupt_util::mt_set::MTSet;
use scupt_util::node_id::NID;
use scupt_util::res::Res;
use sedeve_kit::{auto_enable,
                 check, check_begin, check_end, input,
                 input_begin, input_end,
                 output, setup,
                 setup_begin, setup_end};
use sedeve_kit::player::automata;
use tokio::select;

use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{debug, error, info, trace};
use uuid::Uuid;
use crate::channel::{
    ChReceiver,
    ChSender,
    ChSenderRR
};
use crate::msg_dtm_testing::MDTMTesting;
use crate::msg_fuzzy_testing::MFuzzyTesting;
use crate::msg_raft_state::MRaftState;
use crate::raft_config::{RaftConf, RaftConfigEx};
use crate::raft_event::RaftEvent;
use crate::raft_message::{
    MApplyReq, MApplyResp, LogEntry, MAppendReq,
    MAppendResp, MVoteReq, MVoteResp, PreVoteReq, PreVoteResp,
    RAFT_ABSTRACT,
    RaftMessage, MClientReq, MClientResp,
    RCR_OK, RCR_ERR_RESP, RAFT_FUZZY, RAFT};
use crate::raft_state::RaftState;
use crate::sm_store::{SMStore, WriteEntriesOpt, WriteSnapshotOpt};
use crate::snapshot::{Snapshot, SnapshotIndexTerm};

pub struct StateMachine<T: MsgTrait + 'static> {
    inner: Arc<Mutex<StateMachineInner<T>>>,
}






struct StateMachineInner<T: MsgTrait + 'static> {
    state: RaftState,
    current_term: u64,
    // log entry index started from 1, zero as an invalid index
    // to map the index value to vector's offset, the index value should minus 1
    next_index: HashMap<NID, u64>,
    match_index: HashMap<NID, u64>,
    pre_vote_response: HashMap<NID, PreVoteResp>,
    vote_granted: HashSet<NID>,
    commit_index: u64,
    log: Vec<LogEntry<T>>,
    voted_for: Option<NID>,
    snapshot: SnapshotIndexTerm,
    config: RaftConfigEx,
    max_append_entries: u64,
    store: Arc<dyn SMStore<T>>,
    sender: Arc<dyn Sender<RaftMessage<T>>>,
    receiver: Arc<dyn ReceiverRR<RaftMessage<T>>>,
    client_req:HashMap<String, (NID, Arc<dyn SenderResp<RaftMessage<T>>>)>,
    tick: u64,
    _sender_rr: Arc<dyn SenderRR<RaftMessage<T>>>,
    _notifier:Notifier,

    testing_is_crash:bool,
    _incoming_messages:HashMap<u64, Vec<(
        SnapshotIndexTerm, Vec<LogEntry<T>>,
        Option<NID>,
        u64,
        u64,
        HashMap<NID, u64>,
        RaftState,
        Message<RaftMessage<T>>)>>,
}

const MAX_APPEND_ENTRIES: u64 = 2;


impl<T: MsgTrait + 'static> StateMachine<T> {
    pub fn new(
        conf: RaftConf,
        store: Arc<dyn SMStore<T>>,
        sender: Arc<dyn ChSender<T>>,
        sender_rr:Arc<dyn ChSenderRR<T>>,
        receiver: Arc<dyn ChReceiver<T>>,
    ) -> Self {
        let inner = StateMachineInner::new(
            RaftConfigEx::new(conf), store,
            sender, sender_rr, receiver);
        Self {
            inner: Arc::new(Mutex::new(inner))
        }
    }

    pub async fn recovery(&self) -> Res<()> {
        let mut inner = self.inner.lock().await;
        inner.recovery().await?;
        Ok(())
    }

    pub async fn serve_loop(&self) -> Res<()> {
        let mut inner = self.inner.lock().await;
        inner.handle_incoming().await?;
        Ok(())
    }
}

impl<T: MsgTrait + 'static> StateMachineInner<T> {
    pub fn new(
        conf: RaftConfigEx,
        store: Arc<dyn SMStore<T>>,
        sender: Arc<dyn Sender<RaftMessage<T>>>,
        sender_rr:Arc<dyn SenderRR<RaftMessage<T>>>,
        receiver: Arc<dyn ReceiverRR<RaftMessage<T>>>,
    ) -> Self {

        Self {
            state: RaftState::Follower,
            current_term: Default::default(),
            next_index: Default::default(),
            match_index: Default::default(),
            pre_vote_response: Default::default(),
            vote_granted: Default::default(),
            commit_index: Default::default(),
            log: Default::default(),
            voted_for: None,
            snapshot: Default::default(),
            config: conf,
            max_append_entries: MAX_APPEND_ENTRIES,
            store,
            sender,
            _sender_rr: sender_rr,
            receiver,
            client_req: Default::default(),
            tick:  0,
            _notifier: Default::default(),

            testing_is_crash: false,
            _incoming_messages: HashMap::new(),
        }
    }

    async fn handle_incoming(&mut self) -> Res<()> {
        debug!("state machine node:{} serve loop", self.node_id());
        let ms_tick = self.config.conf().ms_tick;
        if ms_tick > 0 {
            loop {
                select! {
                    r = self.receiver.receive() => {
                        let (m, s) = r?;
                        self.handle_message(m, s).await?;
                    },
                    _ = sleep(Duration::from_millis(ms_tick)) => {
                        self.tick().await?;
                    },
                }
            }
        } else {
            // only available when in deterministic testing
            loop {
                let r = self.receiver.receive().await;
                let (m, s) = r?;
                self.handle_message(m, s).await?;
            }
        }
    }



    async fn tick(&mut self) -> Res<()> {
        if self.state == RaftState::Leader {
            self.tick_leader().await?;
        } else {
            self.tick_non_leader().await?;
        }
        if self.snapshot.index < self.commit_index {
            let index = min(self.snapshot.index + self.config.conf().max_compact_entries, self.commit_index);
            self.compact_log(index).await?;
        }
        Ok(())
    }

    async fn tick_leader(&mut self)  -> Res<()> {
        assert_eq!(self.state, RaftState::Leader);
        self.append_entries().await?;
        Ok(())
    }

    async fn tick_non_leader(&mut self) -> Res<()> {
        if self.tick > self.config.conf().timeout_max_tick {
            self.pre_vote_request().await?;
        }
        self.tick += 1;
        Ok(())
    }

    async fn handle_raft_message(
        &mut self,
        source: NID,
        dest: NID,
        msg: RaftMessage<T>,
        s:Arc<dyn SenderResp<RaftMessage<T>>>
    ) -> Res<()> {

        if self.testing_is_crash  {
            // disable input
            if let RaftMessage::FuzzyTesting(MFuzzyTesting::Restart) = msg {
               self.testing_is_crash = false;
            } else {
                return Ok(())
            }
        }

        assert_eq!(self.node_id(), dest);
        let _a = Message::new(msg.clone(), source, dest);
        match msg {
            RaftMessage::VoteReq(m) => {
                event_add!(RAFT_FUZZY, Message::new(RaftEvent::VoteReq, source, dest));
                input!(RAFT, _a);
                self.handle_vote_req(m).await?;
            }
            RaftMessage::VoteResp(m) => {
                event_add!(RAFT_FUZZY, Message::new(RaftEvent::VoteResp, source, dest));
                input!(RAFT, _a);
                self.handle_vote_resp(m).await?;
            }
            RaftMessage::AppendReq(m) => {
                event_add!(RAFT_FUZZY, Message::new(RaftEvent::AppendReq, source, dest));
                input!(RAFT, _a);
                self.handle_append_req(m).await?;
            }
            RaftMessage::AppendResp(m) => {
                event_add!(RAFT_FUZZY, Message::new(RaftEvent::AppendResp, source, dest));
                input!(RAFT, _a);
                self.handle_append_resp(m).await?;
            }
            RaftMessage::PreVoteReq(_m) => {
                event_add!(RAFT_FUZZY, Message::new(RaftEvent::PreVoteReq, source, dest));
                self.handle_pre_vote_request(_m).await?;
            }
            RaftMessage::PreVoteResp(_m) => {
                event_add!(RAFT_FUZZY, Message::new(RaftEvent::PreVoteResp, source, dest));
                self.handle_pre_vote_response(_m).await?;
            }
            RaftMessage::ApplyReq(_m) => {
                event_add!(RAFT_FUZZY, Message::new(RaftEvent::ApplyReq, source, dest));
                input!(RAFT, _a);
                self.handle_apply_snapshot_request(source, dest, _m).await?;
            }
            RaftMessage::ApplyResp(_m) => {
                event_add!(RAFT_FUZZY, Message::new(RaftEvent::ApplyResp, source, dest));
                input!(RAFT, _a);
                self.handle_apply_snapshot_response(source, dest, _m).await?;
            }
            RaftMessage::ClientReq(m) => {
                self.handle_client_value_req(source, m, s).await?;
            }
            RaftMessage::ClientResp(m) => {
                self.handle_client_response(source, m).await?;
            }
            RaftMessage::DTMTesting(_m) => {
                self.handle_dtm_testing_message(source, dest, _m).await?;
            }
            RaftMessage::FuzzyTesting(m) => {
                self.handle_fuzzy_testing(source, dest, m).await?;
            }
        }
        Ok(())
    }

    async fn handle_message(&mut self, m: Message<RaftMessage<T>>, s:Arc<dyn SenderResp<RaftMessage<T>>>) -> Res<()> {
        let source = m.source();
        let dest = m.dest();
        let _m = m.clone();
        self.handle_raft_message(source, dest, m.payload(), s).await?;

        let _s = (self.snapshot.clone(), self.log.clone(),
         self.voted_for.clone(), self.current_term,
         self.commit_index,
         self.match_index.clone(),
         self.state.clone(),
         _m);
        /*
        if !self._incoming_messages.contains_key(&self.current_term) {
            self._incoming_messages.insert(self.current_term, vec![s]);
        } else {
            self._incoming_messages.get_mut(&self.current_term).unwrap().push(s);
        }
         */
        Ok(())
    }

    async fn handle_fuzzy_testing(&mut self, _source:NID, _dest:NID, m:MFuzzyTesting) -> Res<()> {
        match m {
            MFuzzyTesting::Restart => {
                self.restart_for_testing().await?;
            }
            MFuzzyTesting::Crash => {
                event_add!(RAFT_FUZZY, Message::new(RaftEvent::Crash, self.node_id(), self.node_id()));
                self.testing_is_crash = true;
            }
        }
        Ok(())
    }
    async fn pre_vote_request(&mut self) -> Res<()> {
        self.tick = 0;
        self.state = RaftState::Follower;
        self.vote_granted.clear();
        self.pre_vote_response.clear();
        if self.config.voter().len() > 1 {
            self.send_pre_vote_request().await?;
        } else {
            self.start_request_vote().await?;
        }
        Ok(())
    }

    async fn send_pre_vote_request(&self) -> Res<()> {
        let last_index = self.last_log_index();
        let last_term = self.last_log_term();
        let term = self.current_term;
        let from = self.node_id();
        for id in self.config.voter() {
            if from != *id {
                let m = Message::new(
                    RaftMessage::PreVoteReq(
                        PreVoteReq {
                            source_nid: from,
                            request_term: term,
                            last_log_term: last_term,
                            last_log_index: last_index,
                        }
                    ),
                    self.node_id(),
                    *id,
                );
                self.send(m).await?;
            }
        }
        Ok(())
    }

    async fn handle_pre_vote_request(
        &mut self,
        m: PreVoteReq,
    ) -> Res<()> {
        let grant = self.can_grant_vote(m.request_term + 1,
                                        m.last_log_index,
                                        m.last_log_term,
                                        m.source_nid);

        let resp = Message::new(
            RaftMessage::PreVoteResp(
                PreVoteResp {
                    source_nid: self.node_id(),
                    request_term: m.request_term,
                    vote_granted: grant
                }
            ),
            self.node_id(),
            m.source_nid,
        );
        self.send(resp).await?;
        Ok(())
    }

    async fn handle_pre_vote_response(
        &mut self,
        m: PreVoteResp,
    ) -> Res<()> {
        if self.current_term == m.request_term && self.state == RaftState::Follower {
            let _ = self.pre_vote_response.insert(m.source_nid, m);
            let opt_can_request_vote = self.try_become_candidate();
            if let Some(can_request_vote) = opt_can_request_vote {
                self.pre_vote_response.clear();
                if can_request_vote {
                    self.start_request_vote().await?;
                }
            }
        }
        Ok(())
    }

    fn try_become_candidate(& self) -> Option<bool> {
        if self.pre_vote_response.len() * 2 > self.config.voter().len() {
            let mut yes = 0;
            let mut no = 0;
            for (_, m) in self.pre_vote_response.iter() {
                if m.vote_granted {
                    yes += 1
                } else {
                    no += 1
                }
            }
            if (1 + yes) * 2 > self.config.voter().len() {
                return Some(true);
            }
            if no * 2 > self.config.voter().len() {
                return Some(false);
            }
        }
        None
    }

    async fn try_become_leader(&mut self) -> Res<()> {
        if self.state == RaftState::Candidate &&
            self.vote_granted.len() * 2 > self.config.voter().len() {
            let granted_nodes = self.vote_granted.len();
            if granted_nodes * 2 > self.config.voter().len() {
                self.become_leader().await?;
            }
        }
        Ok(())
    }

    async fn become_leader(&mut self) -> Res<()> {
        event_add!(RAFT_FUZZY, Message::new(RaftEvent::BecomeLeader, self.node_id(), self.node_id()));
        self.next_index.clear();
        self.match_index.clear();
        self.tick = 0;
        self.state = RaftState::Leader;
        self.commit_index = self.snapshot.index;
        let last_index = self.last_log_index();
        for i in self.config.secondary() {
            if *i != self.node_id() {
                self.next_index.insert(*i, last_index + 1);
                self.match_index.insert(*i, self.snapshot.index);
            }
        }

        Ok(())
    }

    fn last_log_term(&self) -> u64 {
        if self.log.is_empty() {
            self.snapshot.term
        } else {
            self.log[self.log.len() - 1].term
        }
    }

    fn last_log_index(&self) -> u64 {
        if self.log.is_empty() {
            self.snapshot.index
        } else {
            self.log[self.log.len() - 1].index
        }
    }


    async fn start_request_vote(&mut self) -> Res<()> {
        // update current term
        self.current_term += 1;

        self.voted_for = Some(self.node_id());

        // save voted_for and current_term
        self.store.set_term_voted(
           self.current_term,
           self.voted_for).await?;

        // become candidate
        self.state = RaftState::Candidate;

        // vote for itself node
        self.vote_granted.insert(self.node_id());

        if self.config.voter().len() > 1 {
            self.send_vote_request().await?;
        } else {
            self.become_leader().await?;
        }
        Ok(())
    }

    async fn send_vote_request(&self) -> Res<()> {
        let last_index = self.last_log_index();
        let last_term = self.last_log_term();
        let term = self.current_term;
        let from = self.node_id();
        for id in self.config.voter() {
            if from != *id {
                let m = Message::new(
                    RaftMessage::VoteReq(
                        MVoteReq {
                            source_nid: self.node_id(),
                            term,
                            last_log_term: last_term,
                            last_log_index: last_index,
                        }
                    ),
                    self.node_id(),
                    *id,
                );
                let _m = m.clone();

                self.send(m).await?;
            }
        }
        Ok(())
    }
    async fn handle_vote_req(
        &mut self,
        m: MVoteReq,
    ) -> Res<()> {
        self.update_term(m.term).await?;
        self.handle_vote_req_gut(m).await?;
        Ok(())
    }

    async fn handle_vote_req_gut(
        &mut self,
        m: MVoteReq,
    ) -> Res<()> {
        let resp = self.vote_req_resp(m).await?;
        self.send(resp).await?;
        Ok(())
    }
    async fn vote_req_resp(
        &mut self,
        m: MVoteReq,
    ) -> Res<Message<RaftMessage<T>>> {
        assert!(self.current_term >= m.term);

        let grant = self.can_grant_vote(m.term,
                                        m.last_log_index,
                                        m.last_log_term,
                                        m.source_nid);
        if grant {
            self.voted_for = Some(m.source_nid);
            self.store.set_term_voted(self.current_term, self.voted_for).await?;
        }
        assert!(grant && self.current_term == m.term || !grant);
        let resp = Message::new(
            RaftMessage::VoteResp(
                MVoteResp {
                    source_nid: self.node_id(),
                    term: self.current_term,
                    vote_granted: grant,
                }
            ),
            self.node_id(),
            m.source_nid,
        );

        Ok(resp)
    }
    async fn handle_vote_resp(
        &mut self,
        m: MVoteResp,
    ) -> Res<()> {
        self.update_term(m.term).await?;
        self.handle_vote_resp_gut(m).await?;
        Ok(())
    }

    async fn handle_vote_resp_gut(
        &mut self,
        m: MVoteResp,
    ) -> Res<()> {
        let _a = Message::new(
            RaftMessage::DTMTesting(MDTMTesting::<T>::HandleVoteResp(m.clone())),
            self.node_id(), self.node_id());

        if m.term == self.current_term && (
            self.state == RaftState::Candidate ||
                self.state == RaftState::Leader) {
            if m.vote_granted {
                self.vote_granted.insert(m.source_nid);
            }
            self.try_become_leader().await?;
        }

        Ok(())
    }

    async fn restart_for_testing(
        &mut self,
    ) -> Res<()> {
        event_add!(RAFT_FUZZY, Message::new(RaftEvent::Restart, self.node_id(), self.node_id()));
        self.testing_is_crash = false;
        self.state = RaftState::Follower;
        self.tick = 0;
        self.match_index.clear();
        self.next_index.clear();
        self.vote_granted.clear();

        self.pre_vote_response.clear();
        self.recovery().await?;
        self.commit_index = self.snapshot.index;
        Ok(())
    }

    async fn compact_log(&mut self, index: u64) -> Res<()> {
        if index <= self.snapshot.index {
            return Ok(());
        }
        event_add!(RAFT_FUZZY, Message::new(RaftEvent::CompactLog, self.node_id(), self.node_id()));
        let n = (index - self.snapshot.index) as usize;
        assert!(n >= 1);
        if cfg!(debug_assertions) {
            let entry = self.store.read_log_entries(0, i64::MAX as u64).await?;
            assert_eq!(entry, self.log);
        }
        let log_entries: Vec<_> = self.log.splice(0..=n - 1, vec![]).collect();
        if !log_entries.is_empty() {
            let entry: &LogEntry<T> = log_entries.last().unwrap();
            self.snapshot.index = entry.index;
            self.snapshot.term = entry.term;

            self.store.compact_log(self.snapshot.clone(), log_entries).await?;
            if cfg!(debug_assertions) {
                let entry = self.store.read_log_entries(0, i64::MAX as u64).await?;
                assert_eq!(entry, self.log);
            }
        }
        Ok(())
    }

    fn is_last_log_term_index_ok(&self, last_index: u64, last_term: u64) -> bool {
        if self.log.len() == 0 {
            let term = self.snapshot.term;
            let index = self.snapshot.index;
            last_term > term || (last_term == term && last_index >= index)
        } else {
            let log_off = self.log.len() - 1;
            let term = self.log[log_off].term;
            let index = self.log[log_off].index;
            last_term > term || (last_term == term && last_index >= index)
        }
    }

    fn can_grant_vote(&self, term: u64, last_index: u64, last_term: u64, vote_for: NID) -> bool {
        return self.is_last_log_term_index_ok(last_index, last_term) &&
            (term > self.current_term ||
                (term == self.current_term &&
                    (match self.voted_for {
                        Some(n) => { n == vote_for }
                        None => { true }
                    })
                )
            );
    }

    async fn append_entries(&self) -> Res<()> {
        for node_id in self.config.secondary() {
            if *node_id != self.node_id() {
                self.append_entries_to_node(node_id.clone()).await?;
            }
        }
        Ok(())
    }

    async fn append_entries_to_node(&self, id: NID) -> Res<()> {
        let rm = self.make_replicate_message(id).await?;
        let m = Message::new(
            rm,
            self.node_id(),
            id,
        );
        self.send(m).await?;
        Ok(())
    }

    fn prev_log_index_of_node(&self, id: NID) -> u64 {
        let opt_i = self.next_index.get(&id);
        let i = match opt_i {
            Some(i) => {
                i.clone()
            }
            None => {
                // the default value of next_index is last_log_index + 1
                self.last_log_index() + 1
            }
        };
        if i >= 1 {
            i - 1
        } else {
            error!("next index must >= 1");
            0
        }
    }

    fn log_term(&self, index: u64) -> u64 {
        if index <= self.snapshot.index {
            return self.snapshot.term;
        } else {
            let i = (index - self.snapshot.index - 1) as usize;
            if i < self.log.len() {
                return self.log[i].term;
            } else {
                panic!("log index error");
            }
        }
    }

    // select log entries started  from index with max entries
    fn select_log_entries(&self, index: u64, max: u64) -> Vec<LogEntry<T>> {
        let mut vec = vec![];
        if index <= self.snapshot.index {
            panic!("error log index, must >= 1");
        }
        let start = (index - self.snapshot.index - 1) as usize;
        let end = min(start + max as usize, self.log.len());
        for i in start..end {
            vec.push(self.log[i].clone())
        }
        return vec;
    }

    async fn make_replicate_message(&self, id: NID) -> Res<RaftMessage<T>> {
        let prev_log_index = self.prev_log_index_of_node(id);
        if prev_log_index < self.snapshot.index {
            let term = self.current_term;
            let id = "".to_string();
            let (snapshot, values, _) = self.store.read_snapshot(id.clone(), None).await?;
            assert_eq!(self.snapshot, snapshot);
            let snapshot = Snapshot {
                term: self.snapshot.term,
                index: self.snapshot.index,
                value: MTSet::new(values.into_iter().collect()),
            };
            Ok(RaftMessage::ApplyReq(
                MApplyReq {
                    source_nid: self.node_id(),
                    term,
                    id,
                    snapshot,
                    iter: vec![],
                }))
        } else {
            let term = self.current_term;
            let prev_log_term = self.log_term(prev_log_index);
            let commit_index = self.commit_index;
            let log_entries = self.select_log_entries(prev_log_index + 1, self.max_append_entries);
            if let Some(l) = log_entries.first() {
                assert_eq!(l.index, prev_log_index + 1);
            }
            let m = MAppendReq {
                source_nid: self.node_id(),
                term,
                prev_log_index,
                prev_log_term,
                log_entries,
                commit_index,
            };
            Ok(RaftMessage::AppendReq(m))
        }
    }

    fn is_prev_log_entry_ok(&self, prev_log_index: u64, prev_log_term: u64) -> bool {
        if prev_log_index == 0 &&
            self.snapshot.index == 0 &&
            self.log.len() == 0 {
            return true;
        }

        if self.snapshot.index >= prev_log_index {
            self.snapshot.index == prev_log_index
                && self.snapshot.term == prev_log_term
        } else {
            let i = (prev_log_index - self.snapshot.index) as usize;
            assert!(i > 0);
            let ret = i <= self.log.len()
                && self.log[i - 1].term == prev_log_term
                && self.log[i - 1].index == prev_log_index;
            ret
        }
    }


    async fn follower_append_entries(&mut self, prev_index: u64, log_entries: Vec<LogEntry<T>>) -> Res<u64> {
        let mut _prev_index = prev_index;
        if log_entries.len() == 0 {
            return Ok(_prev_index);
        }

        let offset_start = (_prev_index - self.snapshot.index) as usize;
        if self.log.len() < offset_start {
            panic!("log index error");
        }
        let (to_append, offset_write) = if self.log.len() == offset_start {
            // *prev_index* is exactly equal to current log
            // append all log entries, start at offset *offset_start*
            event_add!(RAFT_FUZZY, Message::new(RaftEvent::AppendLogAll, self.node_id(), self.node_id()));
            (log_entries, offset_start as u64)
        } else {
            assert!(self.log.len() > offset_start);
            let mut entries_inconsistency_index = None;
            for i in 0..log_entries.len() {
                if self.log.len() == offset_start + i {
                    break;
                }
                let e1 = &self.log[offset_start + i];
                let e2 = &log_entries[i];
                if e1.term != e2.term {
                    // the first inconsistency index
                    // the later log would be truncated
                    entries_inconsistency_index = Some(i);
                    break;
                } else {
                    _prev_index +=1;
                    assert_eq!(_prev_index, e1.index);
                }
                assert_eq!(e1.index, e2.index);
            }
            match entries_inconsistency_index {
                Some(i) => {
                    // *self.log* and *[0..i)* entries of *log_entries* are consistent, and they
                    // not need to be appended; log append start at offset *offset_start + i*
                    let mut to_append = log_entries;
                    // to_append, remove index range 0..i, and left index range i..
                    to_append = to_append.drain(i..).collect();
                    event_add!(RAFT_FUZZY, Message::new(RaftEvent::AppendLogOverwrite, self.node_id(), self.node_id()));
                    (to_append, (offset_start + i) as u64)
                }
                None => {
                    // *self.log* and *log_entries* received are consistent, and the equal prefix entries
                    // are not need to be appended;
                    let pos = self.log.len() - offset_start;
                    let mut to_append = log_entries;
                    let pos = min(pos, to_append.len());
                    let to_append1 = to_append.drain(pos..).collect();
                    event_add!(RAFT_FUZZY, Message::new(RaftEvent::AppendLogIgnoreEqual, self.node_id(), self.node_id()));
                    (to_append1, (offset_start + pos) as u64)
                }
            }
        };

        let match_index = self.write_log_entries(_prev_index, offset_write, to_append).await?;

        Ok(match_index)
    }

    async fn write_log_entries(&mut self, prev_index: u64, offset_write_pos: u64, log_entries: Vec<LogEntry<T>>) -> Res<u64> {
        let pos = offset_write_pos as usize;
        return if self.log.len() < pos || log_entries.is_empty() {
            // match index is prev index
            Ok(prev_index)
        } else {
            let mut to_append = log_entries.clone();
            self.log.drain(pos..);
            self.log.append(&mut to_append);
            let len = log_entries.len() as u64;
            self.store.write_log_entries(prev_index, log_entries, WriteEntriesOpt::default()).await?;
            Ok(prev_index + len)
        };
    }

    async fn handle_append_req(
        &mut self,
        m: MAppendReq<T>,
    ) -> Res<()> {
        self.update_term(m.term).await?;
        self.handle_append_req_gut(m).await?;
        Ok(())
    }

    async fn handle_append_req_gut(
        &mut self,
        m: MAppendReq<T>) -> Res<()> {
        self.tick = 0;
        let opt_resp = self.append_req_resp(m).await?;
        match opt_resp {
            Some(m) => {
                self.send(m).await?;
            }
            None => {}
        }
        Ok(())
    }
    async fn append_req_resp(
        &mut self,
        m: MAppendReq<T>,
    ) -> Res<Option<Message<RaftMessage<T>>>> {

        let log_ok = self.is_prev_log_entry_ok(m.prev_log_index, m.prev_log_term);
        let opt_resp = if self.current_term > m.term || (
            self.current_term == m.term &&
                self.state == RaftState::Follower &&
                !log_ok
        ) {
            event_add!(RAFT_FUZZY, Message::new(RaftEvent::AppendLogReject, self.node_id(), self.node_id()));
            // reject request
            let next_index = if self.snapshot.index > m.prev_log_index {
                // a stale prev_log_index
                event_add!(RAFT_FUZZY,
                Message::new(
                    RaftEvent::AppendLogStaleIndex,
                    self.node_id(),
                    self.node_id()));
                self.snapshot.index
            } else {
                0
            };

            let resp = MAppendResp {
                source_nid: self.node_id(),
                term: self.current_term,
                append_success: false,
                match_index: 0,
                next_index,
            };
            let m = Message::new(
                RaftMessage::AppendResp(resp),
                self.node_id(),
                m.source_nid,
            );
            Some(m)
        } else if self.current_term == m.term &&
            self.state == RaftState::Candidate {
            // *become candidate state
            event_add!(RAFT_FUZZY,
                Message::new(
                    RaftEvent::AppendLogCandidateToFollower,
                    self.node_id(),
                    self.node_id()));
            self.become_follower().await?;
            None
        } else if self.current_term == m.term &&
            self.state == RaftState::Follower &&
            log_ok {
            assert!(self.snapshot.index <= m.prev_log_index);
            // append ok

            let match_index = self.follower_append_entries(m.prev_log_index, m.log_entries).await?;
            if m.commit_index > self.commit_index && m.commit_index <= match_index {
                self.set_commit_index(m.commit_index).await?;
            }
            let resp = MAppendResp {
                source_nid: self.node_id(),
                term: self.current_term,
                append_success: true,
                match_index,
                next_index: 0,
            };
            let m = Message::new(
                RaftMessage::AppendResp(resp),
                self.node_id(),
                m.source_nid,
            );
            Some(m)

        } else {
            // ignore state
            None
        };
        Ok(opt_resp)
    }

    async fn handle_append_resp(
        &mut self,
        m: MAppendResp,
    ) -> Res<()> {
        self.update_term(m.term).await?;
        self.handle_append_resp_gut(m).await?;
        Ok(())
    }

    async fn handle_append_resp_gut(
        &mut self,
        m: MAppendResp,
    ) -> Res<()> {
        if !(self.current_term == m.term && self.state == RaftState::Leader) {
            return Ok(());
        }


        if m.append_success {
            self.next_index.insert(m.source_nid, m.match_index + 1);
            assert!(self.last_log_index() >= m.match_index);
            let opt = self.match_index.insert(m.source_nid, m.match_index);
            let advance_commit = match opt {
                Some(v) => { m.match_index > v }
                None => { true }
            };
            if advance_commit {
                self.advance_commit_index().await?;
            }
        } else {
            let last_log_index = self.last_log_index();
            let opt = self.next_index.get_mut(&m.source_nid);
            match opt {
                Some(next_index) => {
                    if m.next_index > 0 {
                        if last_log_index + 1 < m.next_index {
                            error!("may be this is a stale leader?");
                        }
                        let new_index = min(m.next_index, last_log_index + 1);
                        *next_index = new_index;
                    } else {
                        if *next_index > 1 {
                            *next_index -= 1;
                        }
                    }
                }
                None => {
                    if m.next_index > 0 {
                        self.next_index.insert(m.source_nid, m.next_index);
                    }
                }
            }
        }
        Ok(())
    }

    async fn advance_commit_index(&mut self) -> Res<()> {
        assert_eq!(self.state, RaftState::Leader);
        let mut vec = vec![];
        for (nid, index) in self.match_index.iter() {
            if *nid != self.node_id() { // except this node id
                vec.push(*index);
            }
        }
        // add last index of this nodes
        let index = self.last_log_index();
        vec.push(index);

        // sort by descending order
        vec.sort_by(|a, b| b.cmp(a));
        let len = vec.len();
        if len == self.config.voter().len() && len >= 1 {
            let n = (len + 1) / 2 - 1;
            assert!((n + 1)* 2 > len); // reach majority
            let new_commit_index = vec[n];
            self.set_commit_index(new_commit_index).await?;
        }
        Ok(())
    }

    async fn set_commit_index(&mut self, new_commit_index: u64) -> Res<()> {
        if self.commit_index < new_commit_index {
            self.commit_index = new_commit_index;
            self.store.set_commit_index(new_commit_index).await?;
        }
        Ok(())
    }

    async fn handle_apply_snapshot_request(
        &mut self,
        _from: NID,
        _to: NID,
        m: MApplyReq<T>,
    ) -> Res<()> {
        let term = m.term;
        self.update_term(term).await?;
        self.apply_snapshot_gut(m).await?;
        Ok(())
    }

    async fn apply_snapshot_gut(&mut self, m: MApplyReq<T>) -> Res<()> {

        event_add!(RAFT_FUZZY, Message::new(RaftEvent::ApplySnapshot, self.node_id(), self.node_id()));


        if self.current_term != m.term {
            return Ok(());
        }
        let source = m.source_nid;
        let iter = if m.snapshot.index >= self.last_log_index() && m.snapshot.index > self.commit_index {
            assert_eq!(self.current_term, m.term);

            self.snapshot.index = m.snapshot.index;
            self.snapshot.term = m.snapshot.term;
            self.log.clear();
            let mut opt_write_snap = WriteSnapshotOpt::default();
            opt_write_snap.truncate_right = true;
            opt_write_snap.truncate_left = true;
            let opt_iter = self.store.write_snapshot(
                m.id.clone(),
                m.snapshot.to_snapshot_index_term(),
                m.snapshot.to_value(),
                None,
                opt_write_snap
            ).await?;
            let iter = match opt_iter {
                Some(v) => v,
                None => vec![],
            };
            iter
        } else {
            vec![]
        };


        let resp = Message::new(
            RaftMessage::ApplyResp(
                MApplyResp {
                    source_nid: self.node_id(),
                    term: self.current_term,
                    id: m.id,
                    iter,
                }
            ),
            self.node_id(),
            source,
        );

        self.send(resp).await?;
        Ok(())
    }
    #[allow(dead_code)]
    async fn handle_apply_snapshot_response(
        &mut self,
        _from: NID,
        _to: NID,
        m: MApplyResp,
    ) -> Res<()> {
        if self.current_term != m.term {
            return Ok(());
        }
        Ok(())
    }

    async fn send(&self, m: Message<RaftMessage<T>>) -> Res<()> {
        if self.testing_is_crash {
            // disable output
            return Ok(())
        }
        let _m = m.clone();
        output!(RAFT, _m.clone());
        if auto_enable!(RAFT_ABSTRACT)
            || auto_enable!(RAFT)
        {
            return Ok(());
        }
        if fuzzy_enable!(RAFT_FUZZY) {
            fuzzy_message!(RAFT_FUZZY, _m.clone());
            return Ok(())
        }
        let s = self.sender.send(m, Default::default()).await;
        match s {
            Ok(_) => {

            },
            Err(e) => {
                info!("send message error, {}", e);
            }
        }
        Ok(())
    }


    async fn update_term(&mut self, term: u64) -> Res<()> {

        if self.current_term < term {
            self.current_term = term;
            self.become_follower().await?;
        }

        Ok(())
    }

    async fn become_follower(&mut self) -> Res<()> {
        if self.state != RaftState::Learner {
            self.state = RaftState::Follower;
        }
        self.tick = 0;
        self.voted_for = None;
        self.store.set_term_voted(self.current_term, self.voted_for).await?;
        self.match_index.clear();
        self.next_index.clear();
        self.vote_granted.clear();
        Ok(())
    }

    async fn recovery(&mut self) -> Res<()> {
        let (snapshot, _, _) = self.store.read_snapshot(Uuid::new_v4().to_string(), None).await?;
        self.snapshot = snapshot;
        let end = self.store.get_max_log_index().await? + 1;
        let start = self.store.get_min_log_index().await?;
        let log = self.store.read_log_entries(start, end).await?;
        self.log = log;
        let (term, voted_for) = self.store.get_term_voted().await?;
        self.current_term = term;
        self.voted_for = voted_for;
        trace!("state machine node:{} recovery success", self.node_id());
        Ok(())
    }

    async fn handle_dtm_testing_message(&mut self, source: NID, dest: NID, m: MDTMTesting<T>) -> Res<()> {
        let _a = Message::new(RaftMessage::DTMTesting(m.clone()), source, dest);
        match m {
            MDTMTesting::Setup(hs) => {
                setup!(RAFT, _a.clone());
                setup_begin!(RAFT_ABSTRACT, _a.clone());
                self.state_setup(hs).await?;
                setup_end!(RAFT_ABSTRACT, _a.clone());
            }
            MDTMTesting::Check(hs) => {
                check!(RAFT, _a.clone());
                check_begin!(RAFT_ABSTRACT, _a.clone());
                self.state_check(hs).await?;
                check_end!(RAFT_ABSTRACT, _a.clone());
            }
            MDTMTesting::RequestVote => {
                input!(RAFT, _a.clone());
                input_begin!(RAFT_ABSTRACT, _a.clone());
                self.start_request_vote().await?;
                input_end!(RAFT_ABSTRACT, _a.clone());
            }
            MDTMTesting::BecomeLeader => {
                input_begin!(RAFT_ABSTRACT, _a.clone());
                self.become_leader().await?;
                input_end!(RAFT_ABSTRACT, _a.clone());
            }
            MDTMTesting::AppendLog => {
                input_begin!(RAFT_ABSTRACT, _a.clone());
                input_end!(RAFT_ABSTRACT, _a.clone());
                input!(RAFT, _a.clone());
                self.append_entries().await?;
            }
            MDTMTesting::ClientWriteLog(v) => {
                input!(RAFT, _a.clone());
                input_begin!(RAFT_ABSTRACT, _a.clone());
                let _ = self.client_request_write_value_gut(v).await?;
                input_end!(RAFT_ABSTRACT, _a.clone());
            }
            MDTMTesting::HandleVoteReq(m) => {
                if auto_enable!(RAFT_ABSTRACT) {
                    input_begin!(RAFT_ABSTRACT, _a.clone());
                    // invoke update_term
                    self.handle_vote_req(m).await?;
                    input_end!(RAFT_ABSTRACT, _a.clone());
                } else if auto_enable!(RAFT) {
                    // do not invoke update_term
                    self.handle_vote_req_gut(m).await?;
                }
            }
            MDTMTesting::HandleAppendReq(m) => {
                if auto_enable!(RAFT_ABSTRACT) {
                    input_begin!(RAFT_ABSTRACT, _a.clone());
                    self.handle_append_req(m).await?;
                    input_end!(RAFT_ABSTRACT, _a.clone());
                } else if auto_enable!(RAFT) {
                    self.handle_append_req_gut(m).await?;
                }
            }
            MDTMTesting::UpdateTerm(term) => {
                input_begin!(RAFT_ABSTRACT, _a.clone());
                self.update_term(term).await?;
                input_end!(RAFT_ABSTRACT, _a.clone());
            }
            MDTMTesting::HandleAppendResp(m) => {
                self.handle_append_resp_gut(m).await?;
            }
            MDTMTesting::HandleVoteResp(m) => {
                self.handle_vote_resp_gut(m).await?
            }
            MDTMTesting::Restart => {
                input!(RAFT, _a.clone());
                input_begin!(RAFT_ABSTRACT, _a.clone());
                self.restart_for_testing().await?;
                input_end!(RAFT_ABSTRACT, _a.clone());
            }
            MDTMTesting::AdvanceCommitIndex(index) => {
                input_begin!(RAFT_ABSTRACT, _a.clone());
                self.set_commit_index(index).await?;
                input_end!(RAFT_ABSTRACT, _a.clone());
            }
            MDTMTesting::LogCompaction(index) => {
                input!(RAFT, _a.clone());
                input_begin!(RAFT_ABSTRACT, _a.clone());
                self.compact_log(index).await?;
                input_end!(RAFT_ABSTRACT, _a.clone());
            }
            MDTMTesting::HandleApplyReq(m) => {
                self.apply_snapshot_gut(m).await?;
            }
            MDTMTesting::HandleApplyResp(_m) => {}
        }
        Ok(())
    }

    async fn state_setup(&mut self, hs: MRaftState<T>) -> Res<()> {
        self.current_term = hs.current_term;
        self.voted_for = hs.voted_for;
        self.log = hs.log.clone();
        self.snapshot = hs.snapshot.to_snapshot_index_term();
        self.state = hs.state;
        match hs.log.first() {
            Some(l) => {
                assert_eq!(hs.snapshot.index + 1, l.index);
            }
            None => { }
        };

        self.match_index = hs.match_index.to_map();
        self.next_index = hs.next_index.to_map();
        self.commit_index = hs.commit_index;
        self.vote_granted = hs.vote_granted.to_set();
        self.store.setup_store_state(
            Some(hs.commit_index),
            Some((hs.current_term, hs.voted_for)),
            Some(hs.log),
            Some((hs.snapshot.to_snapshot_index_term(), hs.snapshot.to_value())),
        ).await?;
        Ok(())
    }

    async fn state_check(&self, hs: MRaftState<T>) -> Res<()> {
        let (term, voted) = self.store.get_term_voted().await?;
        assert_eq!(term, hs.current_term);
        assert_eq!(voted, hs.voted_for);

        let begin = self.store.get_min_log_index().await?;
        let end = self.store.get_max_log_index().await? + 1;
        let log = self.store.read_log_entries(begin, end).await?;
        assert_eq!(log, hs.log);

        let (s, values, _payload) = self.store.read_snapshot(
            Uuid::new_v4().to_string(), None).await?;
        assert_eq!(s, hs.snapshot.to_snapshot_index_term());
        assert_eq!(values.len(), hs.snapshot.value.zzz_array.len());
        if auto_enable!(RAFT) {
            assert_eq!(self.commit_index, hs.commit_index);
        }
        Ok(())
    }
    async fn handle_client_response(&mut self, _source_id:NID, m:MClientResp) -> Res<()> {
        if let Some((source_nid, sender)) = self.client_req.remove(&m.id) {
            let msg = Message::new(
                RaftMessage::<T>::ClientResp(m),
                self.node_id(),
                source_nid
            );
            let r = sender.send(msg).await;
            match r {
                Ok(_) => {

                },
                Err(e) => {
                    error!("{}", e);
                }
            }
        }
        Ok(())
    }

    async fn handle_client_value_req(
        &mut self,
        source_id:NID, m:MClientReq<T>,
        sender:Arc<dyn SenderResp<RaftMessage<T>>>
    ) -> Res<()> {
        let need_resp = m.wait_commit || m.wait_write_local;
        let id = m.id.clone();
        let from_client_request= m.from_client_request;
        let r = self.client_req_resp(source_id, m, sender.clone()).await;
        if need_resp {
            let resp = match r {
                Ok(opt) => {
                    match opt {
                        Some(e) => { e }
                        None => {
                            MClientResp {
                                id,
                                source_id: self.node_id(),
                                index: 0,
                                term: 0,
                                error: RCR_ERR_RESP,
                                info: "".to_string(),
                            }
                        }
                    }
                }
                Err(e) => {
                    MClientResp {
                        id,
                        source_id: self.node_id(),
                        index: 0,
                        term: 0,
                        error: RCR_ERR_RESP,
                        info: e.to_string(),
                    }
                }
            };
            let msg = Message::new(
                RaftMessage::ClientResp(resp),
                self.node_id(),
                source_id);

            if from_client_request {
                let r = sender.send(msg).await;
                match r {
                    Ok(_) => {

                    },
                    Err(e) => {
                        error!("{}", e);
                    }
                }
            } else {
                self.send(msg).await?;
            }
            Ok(())
        } else {
            Ok(())
        }
    }

    /*
    async fn client_request_send_to_leader(&mut self, m:MClientReq<T>) -> Res<()> {
        if let Some(leader) = self.voted_for {
            let wait_commit = m.wait_commit;
            let wait_write_local = m.wait_write_local;
            if wait_commit || wait_write_local {
                let mut m = m;
                m.from_client_request = false;
                m.source_id = Some(self.node_id());
                let msg = Message::new(
                    RaftMessage::ClientReq(m),
                    self.node_id(), leader);
                self.send(msg).await?;
            }
        }
        Ok(())
    }
     */

    async fn client_req_resp(
        &mut self,
        _source_nid:NID,
        m:MClientReq<T>,
        _sender:Arc<dyn SenderResp<RaftMessage<T>>>
    ) -> Res<Option<MClientResp>> {
        if self.state != RaftState::Leader {
            return Ok(None)
        }

        let (index, term) = self.client_request_write_value_gut(m.value).await?;
        if m.wait_write_local || m.wait_commit {
            let _m = MClientResp {
                id: m.id,
                source_id: self.node_id(),
                index,
                term,
                error: RCR_OK,
                info: "".to_string(),
            };
            return Ok(Some(_m));
        }

        Ok(None)
    }

    async fn client_request_write_value_gut(&mut self, value:T) -> Res<(u64, u64)> {
        assert_eq!(self.state, RaftState::Leader);
        let last_index = self.last_log_index();
        let index = last_index + 1;
        let term = self.current_term;
        let entry = LogEntry {
            term,
            index,
            value,
        };
        self.write_log_entries(last_index, self.log.len() as u64, vec![entry]).await?;
        if self.config.conf().ms_tick > 0 {
            self.append_entries().await?;
        }
        if self.config.voter().len() == 1 {
            self.advance_commit_index().await?;
        }
        Ok((index, term))
    }
    fn node_id(&self) -> NID {
        self.config.node_id()
    }
}


