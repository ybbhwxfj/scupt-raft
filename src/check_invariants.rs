#[cfg(test)]
pub mod tests {

    use std::collections::HashMap;


    use std::sync::{Arc, Mutex};
    use scc::HashMap as ConcurrentHashMap;
    use lazy_static::lazy_static;
    use num_cpus;
    use rayon::{ThreadPool, ThreadPoolBuilder};
    use scupt_util::message::MsgTrait;
    use scupt_util::node_id::NID;

    use crate::raft_message::LogEntry;
    use crate::raft_state::RaftState;
    use crate::snapshot::Snapshot;

    /// InvariantChecker would check raft invariants
    pub struct InvariantChecker {}

    #[derive(Clone)]
    struct _StateForCheck {
        _path: String,
        state: RaftState,
        snapshot: Snapshot<String>,
        commit_index: u64,
        log: Vec<LogEntry<String>>,
        term: u64,
        voted_for: Option<NID>,
    }


    #[derive(Clone)]
    struct GlobalState {
        // state of each nodes
        node_state: HashMap<String, _StateForCheck>,
        // committed index, term
        committed_index_term: HashMap<u64, u64>,
    }

    impl GlobalState {
        fn new() -> Self {
            Self {
                node_state: (Default::default()),
                committed_index_term: Default::default(),
            }
        }

        fn global_state(cluster_name:String) -> Arc<Mutex<Self>> {
            let opt = GLOBAL_STATE.get(&cluster_name);
            match opt {
                Some(s) => { s.get().clone() }
                None => {
                    let all_state = Arc::new(Mutex::new(GlobalState::new()));
                    let _ = GLOBAL_STATE.insert(cluster_name, all_state.clone());
                    all_state.clone()
                }
            }
        }

        fn change_variables(
            &mut self,
            id: String,
            commit_index:Option<u64>,
            raft_state: Option<RaftState>,
            term_voted_for: Option<(u64, Option<NID>)>,
            snapshot_log: Option<(Snapshot<String>, Vec<LogEntry<String>>)>,
            _check_invariants:bool
        ) {
            let mut _opt_prev_log = None;
            let mut _opt_prev_snapshot = None;
            let mut _opt_prev_state = None;
            let mut _opt_prev_term = None;
            let mut _opt_prev_voted_for = None;
            let mut _newly_created = false;
            if !self.node_state.contains_key(&id) {
                let state = _StateForCheck {
                    _path: id.clone(),
                    state: RaftState::Follower,
                    snapshot: Default::default(),
                    commit_index: 0,
                    log: vec![],
                    term: 0,
                    voted_for: None,
                };
                _newly_created = true;
                let _ = self.node_state.insert(id.clone(), state);
            }
            let opt_state = self.node_state.get_mut(&id);
            let node_state = if let Some(_state) = opt_state {
                _state
            } else {
                panic!("could not create state for node {}", id);
            };
            if let Some(index) = commit_index {
                node_state.commit_index = index;
            }
            if let Some(state) = raft_state {
                if state != node_state.state {
                    _opt_prev_state = Some(node_state.state.clone());
                    node_state.state = state;
                }
            }
            if let Some((_term, _voted)) = term_voted_for {
                if _term != node_state.term {
                    _opt_prev_term = Some(node_state.term);
                    node_state.term = _term;
                }
                if _voted != node_state.voted_for {
                    _opt_prev_voted_for = Some(node_state.voted_for);
                    node_state.voted_for = _voted;
                }
            }
            if let Some((snap, log)) = snapshot_log {
                if !snap.eq(&node_state.snapshot) {
                    _opt_prev_snapshot = Some(node_state.snapshot.clone());
                    node_state.snapshot = snap;
                }
                if !log.eq(&node_state.log) {
                    _opt_prev_log = Some(node_state.log.clone());
                    node_state.log = log
                }
            }
        }
    }

    lazy_static! {
    static ref POOL: ThreadPool =
        ThreadPoolBuilder::new()
            .num_threads(num_cpus::get())
            .build()
            .unwrap();
    static ref GLOBAL_STATE: ConcurrentHashMap<String,Arc<Mutex<GlobalState>>> =
        ConcurrentHashMap::new();
}

    impl _StateForCheck {

    }

    impl InvariantChecker {
        pub fn update_commit_index(
            cluster_name:String,
            id: String,
            commit_index:u64,
        ) {
            let s = GlobalState::global_state(cluster_name);
            let mut guard = s.lock().unwrap();
            let (new_commit_index, log) = {
                let opt = guard.node_state.get_mut(&id);
                let mut new_commit_index = 0;
                let log = if let Some(_state) = opt {
                    if _state.commit_index < commit_index {
                        _state.commit_index = commit_index;
                        new_commit_index = _state.commit_index;
                    }
                    _state.log.clone()
                } else {
                    return;
                };
                (new_commit_index, log)
            };
            for e in log {
                if e.index <= new_commit_index {
                    let opt = guard.committed_index_term.insert(e.index, e.term);
                    if let Some(_term) = opt {
                        assert_eq!(_term, e.term)
                    }
                }
            }
            let state = guard.clone();
            Self::check_invariants(state.node_state, state.committed_index_term);

        }
        pub fn reset_committed(cluster_name:String) {
            let s = GlobalState::global_state(cluster_name);
            let mut guard = s.lock().unwrap();
            guard.committed_index_term.clear();
        }

        pub fn set_and_check_invariants<T: MsgTrait + 'static>(
            cluster_name:String,
            id: String,
            commit_index:Option<u64>,
            raft_state: Option<RaftState>,
            term_voted_for: Option<(u64, Option<NID>)>,
            snapshot_log: Option<(Snapshot<T>, Vec<LogEntry<T>>)>,
            check_invariants:bool
        ) {
            let snapshot_log1 = snapshot_log.map(|(_snapshot, _log)| {
                (
                    _snapshot.map(|t| {
                        serde_json::to_string(t).unwrap()
                    }),
                    _log.iter().map(|e| {
                        e.map(|t| {
                            serde_json::to_string(t).unwrap()
                        })
                    }).collect()
                )
            });
            let state = {
                let s = GlobalState::global_state(cluster_name);
                let mut guard = s.lock().unwrap();
                guard.change_variables(id, commit_index, raft_state, term_voted_for, snapshot_log1, check_invariants);
                guard.clone()
            };
            if check_invariants {
                 Self::check_invariants(state.node_state, state.committed_index_term);
            }
        }

        fn check_invariants(
            node_states: HashMap<String, _StateForCheck>,
            committed_index_term: HashMap<u64, u64>) {
            if true {
                POOL.spawn(move || {
                    Self::_check_invariants(node_states, committed_index_term);
                });
            } else {
                Self::_check_invariants(node_states, committed_index_term);
            }
        }
        fn _check_invariants(
            node_states: HashMap<String, _StateForCheck>,
            committed_index_term: HashMap<u64, u64>) {
            for (_id, state) in node_states.iter() {
                let last_index = {
                    if let Some(_l) = state.log.last() {
                        _l.index
                    } else {
                        state.snapshot.index
                    }
                };
                assert!(state.commit_index <= last_index,
                        "commit index must less than or equal last index");
                for i in 0..state.log.len() {
                    let index = state.log[i].index;
                    let term = state.log[i].term;
                    if i > 0 {
                        assert!(i >= 1);
                        assert_eq!(state.log[i - 1].index + 1, index,
                                   "log entry's index monotonically grow");
                        assert!(state.log[i - 1].term <= term,
                                "log entry's term monotonically grow");
                    } else {
                        assert_eq!(i, 0);
                        assert_eq!(state.snapshot.index + 1, index,
                                   "snapshot and first log entry's index monotonically grow");
                        assert!(state.snapshot.term <= term,
                                "snapshot and first log entry's term monotonically grow")
                    }
                    if index <= state.commit_index {
                        let opt = committed_index_term.get(&index);
                        if let Some(t) = opt {
                            assert_eq!(*t, term, "commit <<index, term>> must be consistency");
                        } else {
                            assert!(false, "commit <<index, term>> must be consistency");
                        }
                    }
                }


                let opt = committed_index_term.get(&state.snapshot.index);
                if let Some(t) = opt {
                    assert_eq!(*t, state.snapshot.term, "commit <<index, term>> must be consistency");
                } else {
                    // assert!(false, "commit <<index, term>> must be consistency");
                }

                for e in state.snapshot.value.to_set() {
                    let opt = committed_index_term.get(&e.index);
                    if let Some(_t) = opt {
                        assert!(*_t <= state.snapshot.term, "commit <<index, term>> must be consistency");
                    } else {
                        //assert!(false, "commit <<index, term>> must be consistency");
                    }
                }
            }
        }
    }
}
