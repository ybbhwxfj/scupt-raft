

#[cfg(test)]
pub(crate) mod tests {
    use scupt_util::message::{Message, MsgTrait};
    use std::sync::mpsc::{channel, Receiver, Sender};
    use std::sync::{Arc, Mutex};
    use scupt_fuzzy::fuzzy::FEventMsgHandler;
    pub struct EventMessageHandler<M:MsgTrait + 'static> {
        receiver:Arc<Mutex<Option<Receiver<Message<M>>>>>,
        sender: Sender<Message<M>>,
        use_channel:bool
    }

    impl <M:MsgTrait + 'static> EventMessageHandler<M> {
        pub fn new (use_channel:bool) -> Self {
            let (s, r) = channel();
            let r = if use_channel {
                Some(r)
            } else {
                None
            };
            Self {
                receiver:Arc::new(Mutex::new(r)),
                sender:s,
                use_channel,
            }
        }

        pub fn opt_receiver(&self) -> Option<Receiver<Message<M>>> {
            let mut r = None;
            let mut guard = self.receiver.lock().unwrap();
            std::mem::swap(&mut r, &mut guard);
            return r;
        }
    }

    impl <M:MsgTrait + 'static> FEventMsgHandler for EventMessageHandler<M> {
        fn on_handle(&self, _name:String, message: Message<String>) {
            let _m = message.map(|s|{
                let m:M = serde_json::from_str(s.as_str()).unwrap();
                m
            });
            if self.use_channel {
                let _r = self.sender.send(_m);
            }
        }
    }
}

