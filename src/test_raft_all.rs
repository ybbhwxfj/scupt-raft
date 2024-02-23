#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::raft_message::RAFT;
    use crate::test_dtm::tests::{InputType, test_raft_gut};
    use crate::test_path::tests::test_data_path;

    #[test]
    fn test_raft_all_input_from_db() {
        for i in 1..=1 {
            let path = format!("raft_trace_{}.db", i);
            let buf = PathBuf::from(test_data_path(path.clone()).unwrap());
            if !buf.exists() {
                break;
            }
            test_raft_gut(InputType::FromDB(path),
                          2020, 3, RAFT.to_string(), None)
        }
    }
}