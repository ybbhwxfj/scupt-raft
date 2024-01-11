#[cfg(test)]
mod tests {
    use crate::raft_message::RAFT;
    use crate::test_raft_dtm::tests::{InputType, test_raft_gut};

    #[test]
    fn test_raft_1node() {
        let path = format!("raft_1node_trace_1.db");
        test_raft_gut(InputType::FromDB(path.to_string()), 7555, 1, RAFT.to_string(), None)
    }
}