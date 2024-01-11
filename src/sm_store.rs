use async_trait::async_trait;
use scupt_util::message::MsgTrait;
use scupt_util::node_id::NID;
use scupt_util::res::Res;

use crate::raft_config::RaftConf;
use crate::raft_message::LogEntry;
use crate::snapshot::{IndexValue, SnapshotIndexTerm};

/// state machine store write log entries option parameter
pub struct WriteEntriesOpt {
    /// truncate left side of the log writing position
    pub truncate_left: bool,

    /// truncate right side of the log writing position
    pub truncate_right: bool,
}

/// state machine store write snapshot log entries option parameter
pub struct WriteSnapshotOpt {
    /// truncate left side of the snapshot index position
    pub truncate_left: bool,

    /// truncate right side of the snapshot index position
    pub truncate_right: bool,
}

impl Default for WriteEntriesOpt {
    fn default() -> Self {
        Self {
            truncate_right: true,
            truncate_left: false,
        }
    }
}

impl Default for WriteSnapshotOpt {
    fn default() -> Self {
        Self {
            truncate_left: true,
            truncate_right: false,
        }
    }
}
#[async_trait]
pub trait SMStore<T: MsgTrait + 'static> {
    /// update raft configuration
    async fn write_config(&self, conf: RaftConf) -> Res<()>;

    ///
    async fn read_config(&self) -> Res<RaftConf>;

    /// update term and voted_for
    async fn set_term_voted(&self, term: u64, voted_for: Option<NID>) -> Res<()>;

    /// get current (term, voted_for)
    async fn get_term_voted(&self) -> Res<(u64, Option<NID>)>;

    async fn get_max_log_index(&self) -> Res<u64>;

    async fn get_min_log_index(&self) -> Res<u64>;


    /// the option's meaning depends on the application definition
    async fn compact_log(
        &self,
        snapshot: SnapshotIndexTerm,
        entries: Vec<LogEntry<T>>,
    ) -> Res<()>;

    /// write log entries would set the current log by log_entries
    /// log entry is a sequence:
    ///     `<<(index|-> m , .. ), (index |-> m + 1, ..) .. (index |-> n, ..)>>`
    /// where there's `m <= n`
    ///
    /// the prev_index must be `m - 1`
    ///
    /// truncate left side(`<= prev_index`, or `<= m - 1 `) of the log writing position,
    /// if $WriteEntriesOpt::truncate_left$ is true;
    /// truncate right side(`>= prev_index + Len(log_entry`, or `> n`) of the log writing position,
    /// if `WriteEntriesOpt::truncate_right` is true;

    async fn write_log_entries(&self, prev_index: u64, log_entries: Vec<LogEntry<T>>, option: WriteEntriesOpt) -> Res<()>;

    /// retrieve the log entries in range [start, end)
    async fn read_log_entries(&self, start: u64, end: u64) -> Res<Vec<LogEntry<T>>>;

    /// write snapshot
    /// session id
    async fn write_snapshot(
        &self,
        id: String,
        snapshot: SnapshotIndexTerm,
        values: Vec<IndexValue<T>>,
        iter: Option<Vec<u8>>,
        opt_write: WriteSnapshotOpt,
    ) -> Res<Option<Vec<u8>>>;

    /// the option  and the return Vector's meaning depends on the application
    async fn read_snapshot(
        &self,
        id: String,
        option_iter: Option<Vec<u8>>,
    ) -> Res<(
        SnapshotIndexTerm,
        Vec<IndexValue<T>>,
        Option<Vec<u8>>
    )>;

    async fn set_commit_index(&self, index: u64) -> Res<()>;

    async fn setup_store_state(&self,
                               commit_index: Option<u64>,
                               term_voted: Option<(u64, Option<NID>)>,
                               log: Option<Vec<LogEntry<T>>>,
                               snapshot: Option<(SnapshotIndexTerm, Vec<IndexValue<T>>)>,
    ) -> Res<()>;
}