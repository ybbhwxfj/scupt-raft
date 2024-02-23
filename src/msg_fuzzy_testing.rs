use bincode::{Decode, Encode};
use scupt_util::message::MsgTrait;
use serde::{Deserialize, Serialize};
use crate::msg_raft_state::MRaftState;

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
pub enum MFuzzyTesting<T:MsgTrait + 'static> {
    Restart,
    Crash,
    #[serde(bound = "T: MsgTrait")]
    Setup(MRaftState<T>),
}

impl <T:MsgTrait + 'static> MsgTrait for MFuzzyTesting<T> {

}