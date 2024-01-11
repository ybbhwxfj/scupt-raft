use std::fmt::Debug;
use std::hash::Hash;

use bincode::{Decode, Encode};
use scupt_util::message::MsgTrait;
use scupt_util::mt_map::MTMap;
use scupt_util::mt_set::MTSet;
use scupt_util::node_id::NID;
use serde::{Deserialize, Serialize};

use crate::raft_message::LogEntry;
use crate::raft_state::RaftState;
use crate::snapshot::Snapshot;

#[derive(
Clone,
Hash,
PartialEq,
Eq,
Debug,
Serialize,
Deserialize,
Decode,
Encode,
)]
pub struct MRaftState<T: MsgTrait + 'static> {
    pub state: RaftState,
    pub current_term: u64,
    #[serde(bound = "T: MsgTrait")]
    pub log: Vec<LogEntry<T>>,
    #[serde(bound = "T: MsgTrait")]
    pub snapshot: Snapshot<T>,
    pub voted_for: Option<NID>,
    pub vote_granted: MTSet<NID>,
    pub commit_index: u64,
    pub next_index: MTMap<NID, u64>,
    pub match_index: MTMap<NID, u64>,
}

impl<T: MsgTrait + 'static> MsgTrait for MRaftState<T> {}

