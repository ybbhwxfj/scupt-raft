#[cfg(test)]
mod tests {
    use scupt_util::logger::logger_setup;

    use crate::test_config::tests::SECONDS_TEST_RUN_MAX;
    use crate::test_fuzzy_inner;

    #[test]
    fn test_raft_fuzzy() {
        logger_setup("debug");
        test_fuzzy_inner::tests::run_raft_fuzzy(None, None, 3, SECONDS_TEST_RUN_MAX).unwrap();
    }
}
