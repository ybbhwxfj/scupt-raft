#[cfg(test)]
mod tests {
    use std::{io, thread};
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::mpsc::Receiver;
    use std::thread::JoinHandle;

    use scupt_fuzzy::{event_setup, event_unset};
    use scupt_util::message::Message;
    use scupt_util::res::Res;
    use sedeve_kit::action::panic::set_panic_hook;
    use tracing::info;

    use crate::injection::tests::BugInject;
    use crate::raft_event::RaftEvent;
    use crate::raft_message::RAFT;
    use crate::raft_message::RAFT_FUZZY;
    use crate::test_config::tests::{SECONDS_TEST_RUN_MAX, TEST_CASE_MAX};
    use crate::test_dtm::tests::{InputType, StopSink, test_raft_gut};
    use crate::test_event_handler::tests::EventMessageHandler;
    use crate::test_fuzzy_inner::tests;
    use crate::test_path::tests::test_data_path;

    #[test]
    fn test_spec_driven_inject_event_1() {
        run_spec_driven_inject("injection_event_1.json".to_string()).unwrap();
    }

    #[test]
    fn test_spec_driven_inject_event_2() {
        run_spec_driven_inject("injection_event_2.json".to_string()).unwrap();
    }

    #[test]
    fn test_spec_driven_inject_event_3() {
       run_spec_driven_inject("injection_event_3.json".to_string()).unwrap();
    }

    #[test]
    fn test_spec_driven_bug_injection_event_4() {
        run_fuzzy_inject("injection_event_4.json".to_string());
    }

    #[test]
    fn test_fuzzy_bug_injection_event_1() {
        run_fuzzy_inject("injection_event_1.json".to_string());
    }

    #[test]
    fn test_fuzzy_bug_injection_event_2() {
        run_fuzzy_inject("injection_event_2.json".to_string());
    }

    #[test]
    fn test_fuzzy_bug_injection_event_3() {
        run_fuzzy_inject("injection_event_3.json".to_string());
    }

    #[test]
    fn test_fuzzy_bug_injection_event_4() {
        run_fuzzy_inject("injection_event_4.json".to_string());
    }

    fn run_fuzzy_inject(name:String) {
        set_panic_hook();
        let p = test_data_path(name).unwrap();
        tests::run_raft_fuzzy(Some(p), None, 3, SECONDS_TEST_RUN_MAX).unwrap();
    }

    fn run_spec_driven_inject(events_path: String) -> Res<()> {
        for i in 1..TEST_CASE_MAX {
            let stop_sink = StopSink::new();
            let path = format!("raft_spec_driven_{}.db", i);
            let buf = PathBuf::from(test_data_path(path.clone()).unwrap());
            if !buf.exists() {
                break;
            }
            let inject = BugInject::<RaftEvent>::from_path(events_path.clone(), vec![])?;
            let h = EventMessageHandler::<RaftEvent>::new(true);
            let receiver = h.opt_receiver().unwrap();
            event_setup!(RAFT_FUZZY, Arc::new(h));
            let builder = thread::Builder::new();
            let stop_sink1 = stop_sink.clone();
            let join: io::Result<JoinHandle<bool>> = builder.spawn(|| {
                let ret = handle_receiver(receiver, inject, stop_sink1);
                ret
            });
            test_raft_gut(InputType::FromDB(path), 3000, 3, RAFT.to_string(), Some(stop_sink.clone()));
            event_unset!(RAFT_FUZZY);
            let r = join.unwrap().join().unwrap();
            if r {
                return Ok(());
            }
        }
        Ok(())
    }

    fn handle_receiver(receiver: Receiver<Message<RaftEvent>>, inject: BugInject<RaftEvent>, stop_sink: StopSink) -> bool {
        loop {
            let r = receiver.recv();
            if let Ok(m) = r {
                let detect_injected_bug = inject.input(m);
                if detect_injected_bug {
                    stop_sink.stop();
                    info!("inject bug found OK");
                    return true;
                }
            } else {
                return false;
            }
        }
    }
}