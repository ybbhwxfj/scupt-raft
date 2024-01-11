#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::path::PathBuf;
    use scupt_fuzzy::{event_setup, event_unset};
    use crate::injection::tests::write_event_seq;
    use crate::raft_event::RaftEvent;
    use crate::raft_message::{RAFT, RAFT_FUZZY};
    use crate::test_event_handler::tests::EventMessageHandler;
    use crate::test_path::tests::test_data_path;
    use crate::test_raft_dtm::tests::{InputType, test_raft_gut};

    #[test]
    fn test_trace_output_event() {
        for i in 1..5 {
            let name = format!("injection_trace_{}.json", i);
            let buf = PathBuf::from(test_data_path(name.clone()).unwrap());
            if !buf.exists() {
                break;
            }
            let h = EventMessageHandler::<RaftEvent>::new(true);
            let receiver = h.opt_receiver().unwrap();
            event_setup!(RAFT_FUZZY, Arc::new(h));
            test_raft_gut(InputType::FromJsonFile(name),
                          9000, 3, RAFT.to_string(), None);
            event_unset!(RAFT_FUZZY);
            let mut vec = vec![];
            loop {
                let r = receiver.recv();
                match r {
                    Ok(m) => {
                        vec.push(serde_json::to_string(&m).unwrap().to_string());
                    },
                    Err(_) => {
                        break;
                    }
                }
            }
            write_event_seq(format!("/tmp/injection_event_{}.json", i), vec).unwrap();
        }
    }
}