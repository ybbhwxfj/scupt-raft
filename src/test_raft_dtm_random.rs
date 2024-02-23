#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::str::FromStr;
    use std::time::Instant;

    use rand::seq::SliceRandom;
    use regex::Regex;
    use scupt_util::res::Res;
    use walkdir::WalkDir;

    use crate::raft_message::RAFT;
    use crate::test_config::tests::SECONDS_TEST_RUN_MAX;
    use crate::test_dtm::tests::{InputType, test_raft_gut};
    use crate::test_path::tests::test_data_path;

    #[test]
    fn test_raft_1node() {
        let path = format!("raft_1node_trace_1.db");
        test_raft_gut(InputType::FromDB(path.to_string()), 7555, 1, RAFT.to_string(), None)
    }

    #[test]
    fn test_raft_dtm_random() {
        run_dtm_random(SECONDS_TEST_RUN_MAX).unwrap();
    }

    fn run_dtm_random(test_seconds: u64) -> Res<()> {
        let mut rng = rand::thread_rng();
        let regex = Regex::new(r"raft_trace_(?P<n>[0-9]+)\.db$").unwrap();

        let mut vec: Vec<i32> = vec![];
        let path = test_data_path("".to_string())?;
        for entry in WalkDir::new(path) {
            let path = entry.unwrap();
            if path.file_type().is_file() {
                let s = path.file_name().to_str().unwrap().to_string();
                if let Some(caps) = regex.captures(s.as_str()) {
                    let n = i32::from_str(&caps["n"].to_string()).unwrap();
                    vec.push(n);
                }
            }
        }
        let inst = Instant::now();
        if !vec.is_empty() {
            loop {
                let n = vec.choose(&mut rng).unwrap().to_string();
                let path = format!("raft_trace_{}.db", n);
                let buf = PathBuf::from(test_data_path(path.clone()).unwrap());
                if !buf.exists() {
                    break;
                }
                test_raft_gut(InputType::FromDB(path), 2020, 3, RAFT.to_string(), None);
                if inst.elapsed().as_secs() > test_seconds {
                    break;
                }
            }
        }
        Ok(())
    }
}
