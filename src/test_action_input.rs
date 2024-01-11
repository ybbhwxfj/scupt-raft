#[cfg(test)]
mod tests {
    use scupt_util::res::Res;
    use sedeve_kit::player::action_incoming_factory::ActionIncomingFactory;
    use serde_json::{json, Value};
    use serde_json::Number;

    use crate::test_path::tests::test_data_path;

    #[test]
    fn test_input() {
        test("test_trace.json".to_string()).unwrap();
    }

    fn test(case: String) -> Res<()> {
        let path = test_data_path(case)?;
        let input = ActionIncomingFactory::action_incoming_from_json_file(path)?;
        let mut i = 1;
        let mut trace = vec![];
        loop {
            let r = input.next();
            let s = if let Ok(_s) = r {
                _s
            } else {
                break;
            };
            let v = Number::from(i);
            let value: Value = serde_json::from_str(s.as_str()).unwrap();
            trace.push(json!({"id": Value::Number(v), "object": value}));
            i += 1;
        }
        println!("{}", Value::Array(trace));
        Ok(())
    }
}