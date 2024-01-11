use bincode::{Decode, Encode};
use scupt_util::message::MsgTrait;
use serde::{Deserialize, Serialize};

use crate::msg_raft_state::MRaftState;
use crate::raft_message::{MAppendReq, MAppendResp, MApplyReq, MApplyResp, MVoteReq, MVoteResp};

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
pub enum MDTMTesting<T: MsgTrait + 'static> {
    #[serde(bound = "T: MsgTrait")]
    Setup(MRaftState<T>),
    #[serde(bound = "T: MsgTrait")]
    Check(MRaftState<T>),
    RequestVote,
    BecomeLeader,
    AppendLog,
    #[serde(bound = "T: MsgTrait")]
    ClientWriteLog(T),
    HandleVoteReq(MVoteReq),
    HandleVoteResp(MVoteResp),
    #[serde(bound = "T: MsgTrait")]
    HandleAppendReq(MAppendReq<T>),
    HandleAppendResp(MAppendResp),
    UpdateTerm(u64),
    Restart,
    LogCompaction(u64),
    AdvanceCommitIndex(u64),
    #[serde(bound = "T: MsgTrait")]
    HandleApplyReq(MApplyReq<T>),
    HandleApplyResp(MApplyResp),
}

impl<T: MsgTrait + 'static> MsgTrait for MDTMTesting<T> {}