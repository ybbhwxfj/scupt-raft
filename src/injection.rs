
#[cfg(test)]
pub mod tests {
    use std::collections::{HashMap, HashSet};
    use std::fs;
    use std::sync::{Arc, Mutex};

    use scupt_util::message::{Message, MsgTrait};
    use scupt_util::res::Res;
    use serde_json::Value;
    use crate::test_path::tests::test_data_path;
    use scupt_util::node_id::NID;


    pub fn write_event_seq(path:String, v:Vec<String>) -> Res<()> {
        let vec:Vec<Value> = v.iter().map(|s| {
            let v:Value = serde_json::from_str(s.as_str()).unwrap();
            v
        }).collect();

        let string = serde_json::to_string_pretty(&Value::Array(vec)).unwrap();

        fs::write(path, string).unwrap();
        Ok(())
    }

    pub struct BugInject<M:MsgTrait + 'static> {
        inner:Arc<Mutex<BugInjectInner<M>>>,
    }

    struct BugInjectInner<M:MsgTrait + 'static> {
        expected:HashMap<NID, Vec<Message<M>>>,
        incoming:HashMap<NID, Vec<Message<M>>>,
        filter_set:HashSet<M>,
    }

    impl <M:MsgTrait + 'static> BugInject<M> {

        pub fn from_path(path:String, filter:Vec<M>) -> Res<Self> {
            let p = test_data_path(path)?;
            let s = fs::read_to_string(p).unwrap();
            let v:Value = serde_json::from_str(s.as_str()).unwrap();
            let array = v.as_array().unwrap();
            let mut events = vec![];
            for v in array {
                let m:Message<M> = serde_json::from_value(v.clone()).unwrap();
                events.push(m);
            }
            Ok(Self::new(events, filter))
        }
        pub fn new(injected_events: Vec<Message<M>>, filter:Vec<M>) -> Self {
            Self {
                inner: Arc::new(Mutex::new(BugInjectInner::new(injected_events, filter))),
            }
        }

        pub fn input(&self, event: Message<M>) -> bool {
            let mut guard = self.inner.lock().unwrap();
            guard.input_event(event)
        }
    }

    impl <M:MsgTrait + 'static> BugInjectInner<M> {
        pub fn new(injected_events:Vec<Message<M>>, filter:Vec<M>) -> Self {
            let mut expect:HashMap<NID, Vec<Message<M>>> = HashMap::new();
            for e in injected_events {
                if let Some(v) = expect.get_mut(&e.dest()) {
                    v.push(e);
                } else {
                    expect.insert(e.dest(), vec![e]);
                }
            }
            let mut filter_set = HashSet::new();
            for m in filter {
                filter_set.insert(m);
            }
            Self {
                expected: expect,
                incoming: Default::default(),
                filter_set,
            }
        }
        pub fn input_event(&mut self, event:Message<M>) -> bool {
            let nid = event.dest();
            if !self.filter_set.is_empty() && !self.filter_set.contains(event.payload_ref()) {
                return false;
            }
            let opt = self.incoming.get_mut(&nid);
            match opt {
                Some(v) => { v.push(event); }
                None => { self.incoming.insert(nid, vec![event]); }
            }
            let ok = self.check_events_cover(nid);
            return ok;
        }

        pub fn check_events_cover(&mut self, nid: NID) -> bool {
            let opt_expect = self.expected.get(&nid);
            if let Some(expect) = opt_expect {
                if expect.is_empty() {
                    return false;
                }
                let opt_seq = self.incoming.get(&nid);
                if let Some(seq) = opt_seq {
                    if seq.len() >= expect.len() {
                        for (i, m)  in seq.iter().rev().enumerate() {
                            if expect.len() > i {
                                if !expect[expect.len() - 1 - i].payload_ref().eq(m.payload_ref()) {
                                    return false;
                                }
                            } else {
                                return true;
                            }
                        }
                        return true;
                    }
                }
            }

            return false
        }
    }
}