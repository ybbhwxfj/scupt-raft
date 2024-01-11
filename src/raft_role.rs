use bincode::{Decode, Encode};
use scupt_util::message::MsgTrait;
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
pub enum RaftRole {
    Leader,
    Follower,
    Candidate,
    PreCandidate,
    Learner,
}

impl MsgTrait for RaftRole {}