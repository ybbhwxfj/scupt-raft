use bincode::{Decode, Encode};
use scupt_util::message::MsgTrait;
use scupt_util::node_id::NID;
use serde::{Deserialize, Serialize};

type IsAddNode = bool;


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
pub struct NodePeer {
    pub node_id: NID,
    pub addr: String,
    pub port: u16,
    pub can_vote: bool,
}

impl MsgTrait for NodePeer {}

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
pub struct RaftConf {
    pub cluster_name: String,
    pub storage_path: String,
    pub node_id: NID,
    pub bind_address: String,
    pub bind_port: u16,
    pub timeout_max_tick: u64,
    pub ms_tick: u64,
    pub max_compact_entries: u64,
    pub send_value_to_leader: bool,
    pub node_peer: Vec<NodePeer>,
    pub opt_add: Option<(NID, IsAddNode)>,
}

impl MsgTrait for RaftConf {}

pub struct RaftConfigEx {
    conf: RaftConf,
    current_voter: Vec<NID>,
    current_secondary: Vec<NID>,
}

impl RaftConf {
    pub fn new(cluster_name: String,
               node_id: NID,
               storage_path: String,
               address: String,
               port: u16) -> Self {
        Self {
            cluster_name,
            storage_path,
            node_id,
            bind_address: address,
            bind_port: port,
            timeout_max_tick: 500,
            ms_tick: 50,
            max_compact_entries: 10,
            send_value_to_leader: false,
            node_peer: vec![],
            opt_add: None,
        }
    }

    pub fn add_peers(&mut self, node_id: NID, addr: String, port: u16, can_vote: bool) {
        let peer = NodePeer {
            node_id,
            addr,
            port,
            can_vote,
        };
        self.node_peer.push(peer);
    }
}

impl RaftConfigEx {
    pub fn new(conf: RaftConf) -> Self {
        let mut current_voter = vec![];
        let mut current_secondary = vec![];

        for peer in conf.node_peer.iter() {
            if peer.can_vote {
                current_voter.push(peer.node_id.clone());
            }
            current_secondary.push(peer.node_id.clone());
        }
        let rc = RaftConfigEx {
            conf,
            current_voter,
            current_secondary,
        };
        rc
    }

    pub fn conf(&self) -> &RaftConf {
        &self.conf
    }

    pub fn node_id(&self) -> NID {
        self.conf.node_id
    }

    pub fn voter(&self) -> &Vec<NID> {
        &self.current_voter
    }

    pub fn secondary(&self) -> &Vec<NID> {
        &self.current_secondary
    }
}