#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::raft_message::RAFT;
    use crate::test_config::tests::TEST_CASE_MAX;
    use crate::test_path::tests::test_data_path;
    use crate::test_raft_dtm::tests::{InputType, test_raft_gut};

    #[test]
    fn test_raft_replicate_input_from_db() {
        for i in 1..TEST_CASE_MAX {
            let path = format!("raft_replicate_trace_{}.db", i);
            let buf = PathBuf::from(test_data_path(path.clone()).unwrap());
            if !buf.exists() {
                break;
            }
            test_raft_gut(InputType::FromDB(path), 4000, 3, RAFT.to_string(), None);
        }
    }
}