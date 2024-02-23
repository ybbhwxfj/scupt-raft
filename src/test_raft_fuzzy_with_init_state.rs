#[cfg(test)]
mod tests {
    use scupt_util::logger::logger_setup;
    use scupt_util::res::Res;
    use scupt_util::sigpipe_ign::sigpipe_ign;
    use sedeve_kit::trace_gen::trace_db::TraceDB;
    use tracing::info;

    use crate::test_config::tests::SECONDS_TEST_RUN_MAX;
    use crate::test_fuzzy_inner;
    use crate::test_path::tests::test_data_path;

    #[test]
    fn test_raft_fuzzy_with_init_state() {
        logger_setup("debug");
        let path = test_data_path("raft_abstract_init_setup_trace.db".to_string()).unwrap();
        fuzzy_with_init_state(path, 60, SECONDS_TEST_RUN_MAX).unwrap();
    }

    fn fuzzy_with_init_state(path:String, time_per_run_seconds:u64, time_total_seconds:u64) -> Res<()> {
        sigpipe_ign();
        let db = TraceDB::new(path)?;
        let traces = db.read_trace()?;
        if traces.len() == 0 {
            return Ok(())
        }
        let sec = if (traces.len()as u64) * time_per_run_seconds > time_total_seconds {
            time_per_run_seconds
        } else {
            time_total_seconds / (traces.len() as u64)
        };
        let mut total_sec = 0;
        for (i, trace) in traces.iter().enumerate() {
            info!("run fuzzy testing with initialize state {}", i + 1);
            test_fuzzy_inner::tests::run_raft_fuzzy(None, Some(trace.clone()), 3, sec)?;
            total_sec += sec;
            if total_sec >= time_total_seconds {
                break;
            }
        }
        Ok(())
    }
}
