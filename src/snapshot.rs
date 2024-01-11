use bincode::{Decode, Encode};
use scupt_util::message::MsgTrait;
use scupt_util::mt_set::MTSet;
use serde::{Deserialize, Serialize};

#[derive(
Clone,
Eq,
PartialEq,
Debug,
Serialize,
Deserialize,
Decode,
Encode,
)]
pub struct SnapshotIndexTerm {
    pub term: u64,
    pub index: u64,
}

impl SnapshotIndexTerm {
    pub fn new() -> Self {
        Self {
            term: 0,
            index: 0,
        }
    }
}


impl Default for SnapshotIndexTerm {
    fn default() -> Self {
        Self::new()
    }
}

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
pub struct IndexValue<V> {
    pub index: u64,
    #[serde(bound = "V: MsgTrait")]
    pub value: V,
}

impl<V: MsgTrait + 'static> MsgTrait for IndexValue<V> {}

/// SnapshotV use by DTM
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
pub struct Snapshot<V: MsgTrait + 'static> {
    pub term: u64,
    pub index: u64,
    #[serde(bound = "V: MsgTrait")]
    pub value: MTSet<IndexValue<V>>,
}


impl<V: MsgTrait + 'static> MsgTrait for Snapshot<V> {}

impl<V: MsgTrait + 'static> Default for Snapshot<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V: MsgTrait + 'static> Snapshot<V> {
    pub fn new() -> Self {
        Self {
            term: 0,
            index: 0,
            value: MTSet::default(),
        }
    }

    pub fn to_snapshot_index_term(&self) -> SnapshotIndexTerm {
        SnapshotIndexTerm {
            term: self.term,
            index: self.index,
        }
    }

    pub fn to_value(&self) -> Vec<IndexValue<V>> {
        let vec: Vec<IndexValue<V>> = self.value.to_set().into_iter().collect();
        vec
    }

    pub fn map<V2, F>(&self, f: F) -> Snapshot<V2>
        where V2: MsgTrait + 'static,
              F: Fn(&V) -> V2
    {
        let set1 = self.value.to_set();
        let set2 = set1.iter().map(|v| {
            IndexValue {
                index: v.index,
                value: f(&v.value),
            }
        }).collect();

        Snapshot {
            term: self.term,
            index: self.index,
            value: MTSet::new(set2),
        }
    }
}