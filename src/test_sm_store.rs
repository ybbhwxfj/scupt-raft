#[cfg(test)]
mod tests {
    use scupt_util::res::Res;
    use tokio::runtime::Builder;

    use crate::raft_config::RaftConf;
    use crate::raft_message::LogEntry;
    use crate::sm_store::{SMStore, WriteEntriesOpt};
    use crate::test_store_simple::tests::SMStoreSimple;

    #[test]
    fn test_simple_store() {
        let store = SMStoreSimple::create(
            RaftConf::new("test store".to_string(), 1,
                          "/tmp/test_simple_sm_store.db".to_string(),
                          "1.1.1.1".to_string(), 1000),
            false).unwrap();
        let runtime = Builder::new_current_thread().build().unwrap();
        runtime.block_on(async move {
            test_write_log(&store).await.unwrap();
        })
    }

    async fn test_write_log(store: &dyn SMStore<u64>) -> Res<()> {
        write_log_entries(store, 1, 100, true, true).await?;
        write_log_entries(store, 2, 90, true, true).await?;
        Ok(())
    }

    async fn write_log_entries(store: &dyn SMStore<u64>, min: u64, max: u64, truncate_left: bool, truncate_right: bool) -> Res<()> {
        let mut entries = vec![];
        for i in min..=max {
            let e = LogEntry {
                term: 1,
                index: i,
                value: i,
            };
            entries.push(e);
        }

        store.write_log_entries(min - 1, entries, WriteEntriesOpt {
            truncate_left,
            truncate_right,
        }).await?;
        if truncate_left {
            let index_min = store.get_min_log_index().await?;
            assert_eq!(index_min, min);
        }
        if truncate_right {
            let index_max = store.get_max_log_index().await?;
            assert_eq!(index_max, max);
        }
        Ok(())
    }
}