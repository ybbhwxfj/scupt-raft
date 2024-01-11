use scupt_util::message::MsgTrait;
use bincode::{Decode, Encode};

use serde::{Deserialize, Serialize};

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
pub enum RaftEvent {
    PreVoteReq,
    PreVoteResp,
    VoteReq,
    VoteResp,
    AppendReq,
    AppendResp,
    ApplyReq,
    ApplyResp,
    ClientReq,
    ClientResp,
    BecomeLeader,
    Restart,
    Crash,
    CompactLog,
    ApplySnapshot,
    AppendLogReject,
    AppendLogAll,
    AppendLogIgnoreEqual,
    AppendLogOverwrite,
    AppendLogCandidateToFollower,
    AppendLogStaleIndex,
}

impl MsgTrait for RaftEvent {

}

