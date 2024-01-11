#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use scupt_util::message::{Message, test_check_message};
    use scupt_util::message::MsgTrait;
    use scupt_util::mt_set::MTSet;

    use crate::msg_dtm_testing::MDTMTesting;
    use crate::msg_fuzzy_testing::MFuzzyTesting;
    use crate::msg_raft_state::MRaftState;
    use crate::raft_config::{NodePeer, RaftConf};
    use crate::raft_event::RaftEvent;
    use crate::raft_message::{LogEntry, MAppendReq, MAppendResp, MApplyReq, MApplyResp, MClientReq, MClientResp, MVoteReq, MVoteResp, PreVoteReq, PreVoteResp, RaftMessage};
    use crate::raft_state::RaftState;
    use crate::snapshot::{IndexValue, Snapshot};

    #[test]
    fn test_raft_config() {
        let vec = vec![
            RaftConf {
                cluster_name: "".to_string(),
                storage_path: "".to_string(),
                node_id: 0,
                bind_address: "".to_string(),
                bind_port: 0,
                timeout_max_tick: 500,
                ms_tick: 50,
                max_compact_entries: 1,
                send_value_to_leader: false,
                node_peer: vec![NodePeer {
                    node_id: 0,
                    addr: "".to_string(),
                    port: 0,
                    can_vote: false,
                }],
                opt_add: None,
            }
        ];
        test_message(vec);
    }

    #[test]
    fn test_raft_message() {
        let vec = vec![
            RaftMessage::VoteReq(MVoteReq {
                source_nid: 0,
                term: 0,
                last_log_term: 0,
                last_log_index: 0,
            }),
            RaftMessage::VoteResp(MVoteResp {
                source_nid: 0,
                term: 0,
                vote_granted: false,
            }),
            RaftMessage::AppendResp(MAppendResp {
                source_nid: 0,
                term: 0,
                append_success: false,
                match_index: 0,
                next_index: 0,
            }),
            RaftMessage::AppendReq(MAppendReq {
                source_nid: 0,
                term: 0,
                prev_log_index: 0,
                prev_log_term: 0,
                log_entries: vec![
                    LogEntry {
                        term: 0,
                        index: 0,
                        value: 1,
                    }
                ],
                commit_index: 0,
            }),
            RaftMessage::ApplyReq(MApplyReq {
                source_nid: 0,
                term: 0,
                id: "".to_string(),
                snapshot: Snapshot {
                    term: 0,
                    index: 0,
                    value: MTSet::new(
                        HashSet::from_iter(vec![
                            IndexValue {
                                index: 1,
                                value: 1,
                            },
                            IndexValue {
                                index: 2,
                                value: 2,
                            }].iter().cloned())),
                },
                iter: vec![],
            }),
            RaftMessage::ApplyResp(MApplyResp {
                source_nid: 0,
                term: 0,
                id: "".to_string(),
                iter: vec![],
            }),
            RaftMessage::PreVoteReq(PreVoteReq {
                source_nid: 0,
                request_term: 0,
                last_log_term: 0,
                last_log_index: 0,
            }),
            RaftMessage::PreVoteResp(PreVoteResp {
                source_nid: 0,
                request_term: 0,
                vote_granted: false,
            }),
            RaftMessage::DTMTesting(MDTMTesting::Check(MRaftState {
                state: RaftState::Leader,
                current_term: 0,
                log: vec![],
                snapshot: Default::default(),
                voted_for: None,
                vote_granted: Default::default(),
                commit_index: 0,
                next_index: Default::default(),
                match_index: Default::default(),
            })),
            RaftMessage::DTMTesting(MDTMTesting::Setup(MRaftState {
                state: RaftState::Follower,
                current_term: 1,
                log: vec![],
                snapshot: Default::default(),
                voted_for: None,
                vote_granted: Default::default(),
                commit_index: 2,
                next_index: Default::default(),
                match_index: Default::default(),
            })),
            RaftMessage::DTMTesting(MDTMTesting::AdvanceCommitIndex(1)),
            RaftMessage::DTMTesting(MDTMTesting::AppendLog),
            RaftMessage::DTMTesting(MDTMTesting::BecomeLeader),
            RaftMessage::DTMTesting(MDTMTesting::ClientWriteLog(1)),
            RaftMessage::DTMTesting(MDTMTesting::HandleAppendReq(MAppendReq {
                source_nid: 0,
                term: 0,
                prev_log_index: 0,
                prev_log_term: 0,
                log_entries: vec![],
                commit_index: 0,
            })),
            RaftMessage::DTMTesting(MDTMTesting::HandleAppendResp(MAppendResp {
                source_nid: 0,
                term: 0,
                append_success: false,
                match_index: 0,
                next_index: 0,
            })),
            RaftMessage::DTMTesting(MDTMTesting::HandleVoteReq(MVoteReq {
                source_nid: 0,
                term: 0,
                last_log_term: 0,
                last_log_index: 0,
            })),
            RaftMessage::DTMTesting(MDTMTesting::HandleVoteResp(MVoteResp {
                source_nid: 0,
                term: 0,
                vote_granted: false,
            })),
            RaftMessage::DTMTesting(MDTMTesting::HandleAppendReq(MAppendReq {
                source_nid: 0,
                term: 0,
                prev_log_index: 0,
                prev_log_term: 0,
                log_entries: vec![],
                commit_index: 0,
            })),
            RaftMessage::DTMTesting(MDTMTesting::HandleApplyResp(MApplyResp {
                source_nid: 0,
                term: 0,
                id: "".to_string(),
                iter: vec![],
            })),
            RaftMessage::ClientReq(MClientReq {
                id: "".to_string(),
                value: 2,
                source_id: None,
                wait_write_local: false,
                wait_commit: false,
                from_client_request: false,
            }),
            RaftMessage::ClientResp(MClientResp{
                id: "".to_string(),
                source_id: 1,
                index: 1,
                term: 1,
                error: 0,
                info: "".to_string(),
            }),
            RaftMessage::DTMTesting(MDTMTesting::LogCompaction(10)),
            RaftMessage::DTMTesting(MDTMTesting::Restart),
            RaftMessage::FuzzyTesting(MFuzzyTesting::Restart),
            RaftMessage::FuzzyTesting(MFuzzyTesting::Crash),
        ];
        test_message(vec);
        let vec = vec![
            RaftEvent::ClientReq,
        ];
        test_message(vec);
    }

    fn test_message<M: MsgTrait + 'static>(vec: Vec<M>) {
        for m in vec {
            let msg = Message::new(m, 1, 2);
            let ok = test_check_message(msg);
            assert!(ok);
        }
    }
}