#[cfg(test)]
mod tests {
    use scupt_util::res::Res;
    use sedeve_kit::action::action_message::ActionMessage;
    use sedeve_kit::action::tla_actions::TLAActionSeq;
    use sedeve_kit::trace_gen::action_from_state_db::read_actions;
    use sedeve_kit::trace_gen::read_json::read_from_dict_json;
    use serde_json::Value;
    use tracing::trace;

    use crate::raft_message::RaftMessage;
    use crate::test_path::tests::test_data_path;

    #[test]
    fn test_read() {
        let path = test_data_path("raft_map_const.json".to_string()).unwrap();
        let map = read_from_dict_json(Some(path.clone())).unwrap();
        let f = |v: Value| -> Res<()> {
            let tla_action_seq = TLAActionSeq::from(v.clone())?;
            for vec in [tla_action_seq.actions(), tla_action_seq.states()] {
                for a in vec {
                    let j = a.to_action_json()?;
                    let s = j.to_action_message();
                    let _a: ActionMessage<RaftMessage<u64>> = serde_json::from_str(s.to_string().unwrap().as_str()).unwrap();
                    trace!("{:?}", _a);
                }
            }
            Ok(())
        };
        let path = test_data_path("raft_abstract_action.db".to_string()).unwrap();
        read_actions(path.to_string(), &map, &f).unwrap();
    }
}