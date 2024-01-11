
#[cfg(test)]
pub mod tests {
    use std::collections::HashMap;
    use std::{fs, thread};
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    use std::path::PathBuf;

    use std::str::FromStr;
    use std::sync::{Arc, Mutex};
    use std::thread::JoinHandle;
    use std::time::{Duration, Instant};
    use rand::seq::{IteratorRandom, SliceRandom};
    use rand::{Rng, thread_rng};
    use rand_distr::Normal;
    use scupt_fuzzy::fuzzy_command::FuzzyCommand;
    use scupt_fuzzy::fuzzy_event::FuzzyEvent;
    use scupt_fuzzy::fuzzy_event::FuzzyEvent::{Crash, Delay, Duplicate, Lost, Restart};
    use scupt_fuzzy::fuzzy_generator::FuzzyGenerator;
    use scupt_net::notifier::Notifier;
    use scupt_util::logger::{logger_setup, logger_setup_with_console};
    use scupt_util::node_id::NID;
    use scupt_util::res::Res;
    use scupt_util::res_of::res_io;
    use scupt_net::client::{Client, OptClientConnect};
    use scupt_util::error_type::ET;
    use scupt_util::message::{Message, MsgTrait};
    use tokio::runtime::Runtime;
    use tokio::select;
    use tokio::task::LocalSet;
    use tokio::time::sleep;
    use tracing::{error, info};
    use scupt_fuzzy::fuzzy_server::FuzzyServer;
    use scupt_fuzzy::{event_setup, fuzzy_setup, fuzzy_unset};

    use scupt_net::task::spawn_local_task;

    use std::sync::mpsc::Receiver;
    use rand::rngs::ThreadRng;

    use crate::raft_config::NodePeer;
    use crate::raft_message::{MClientReq, RAFT_FUZZY, RaftMessage};
    use crate::test_raft_node::tests::TestRaftNode;
    use uuid::Uuid;
    use crate::injection::tests::BugInject;

    use crate::msg_fuzzy_testing::MFuzzyTesting;

    use crate::raft_event::RaftEvent;
    use crate::test_event_handler::tests::EventMessageHandler;
    use crate::test_config::tests::{SECONDS_FUZZY_RUN_MAX, TEST_LOCK};

    #[derive(Clone)]
    struct TestRaftFuzzy<M:MsgTrait + 'static> {
        node_db_path: String,
        test_nodes: Vec<NodePeer>,
        node_peers: Vec<NodePeer>,
        fuzzy_server_listen:NodePeer,
        fuzzy_client_connect_to_server:NodePeer,
        fuzzy_server_notifier:Notifier,
        request_notifier:Notifier,
        server_notifiers: Vec<Notifier>,
        client_notifiers: Vec<Notifier>,
        clients: Vec<(Client<RaftMessage<M>>, NID)>,
        _peers: HashMap<NID, SocketAddr>,
        _nodes: Arc<Mutex<HashMap<NID, TestRaftNode>>>,
        receiver:Arc<Mutex<Receiver<Message<RaftEvent>>>>,
        handler:Arc<EventMessageHandler<RaftEvent>>,
        event_control : Arc<Mutex<EventCtrl>>
    }

    struct EventCtrl {
        cover_event:HashMap<RaftEvent, u64>
    }

    impl EventCtrl {
        fn new() -> Self {
            let mut map = HashMap::new();
            let vec = [
                RaftEvent::Restart,
                RaftEvent::Crash,
                RaftEvent::BecomeLeader,
                RaftEvent::AppendLogOverwrite,
                RaftEvent::AppendLogIgnoreEqual,
                RaftEvent::AppendLogAll,
                RaftEvent::ApplySnapshot,
                RaftEvent::AppendLogCandidateToFollower,
                RaftEvent::AppendLogStaleIndex,
                RaftEvent::AppendLogReject
            ];
            for e in vec {
                map.insert(e, 0);
            }
            Self {
                cover_event:map
            }
        }

        fn print_not_covered_event(&self) {
            for (k, v) in &self.cover_event {
                if *v == 0 {
                    info!("event {:?}", k);
                }
            }
        }

        fn event_cover(&mut self, event:Message<RaftEvent>) -> bool {
            let test = {
                let opt = self.cover_event.get_mut(event.payload_ref());
                match opt {
                    Some(i) => {
                        if *i == 0 {
                            info!("event: {:?}", event);
                        }
                        *i += 1;
                        true
                    }
                    _ => {
                        false
                    }
                }
            };
            if test {
                self.all_covered()
            } else {
                false
            }
        }

        fn all_covered(&self) -> bool{
            for (_k, v) in &self.cover_event {
                if *v == 0 {
                    return false;
                }
            }
            return true;
        }
    }

    struct Generator {
        crash_ratio:f64,
        message_max_repeat:u64,
        mean_delayed_millis:u64,
        message_delay_ratio:f64,
        message_repeat_ratio:f64,
        message_lost_ratio:f64,
        network_partition:f64,
        node_id:Vec<NID>,
    }

    fn to_sock_addr(p:&NodePeer) -> SocketAddr {
        let addr = SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::from_str(p.addr.as_str()).unwrap(),
            p.port
        ));
        addr
    }

    impl Generator {
        fn new(vec:Vec<NID>) -> Self {
            Self {
                crash_ratio:0.001,
                message_delay_ratio:0.10,
                message_repeat_ratio:0.10,
                message_lost_ratio:0.10,
                message_max_repeat:10,
                network_partition:0.001,
                mean_delayed_millis:50,
                node_id:vec
            }
        }

        fn gen_rand_nid_set(&self, rng: &mut ThreadRng) -> Vec<NID> {
            let n1 = rng.gen_range(1..=self.node_id.len()/2);
            let ids1:Vec<NID> = self.node_id.iter()
                .choose_multiple(rng, n1)
                .iter()
                .map(|i|{**i})
                .collect();
            ids1
        }
        fn gen_partition_fuzzy(&self) -> Vec<FuzzyEvent> {
            let mut rnd = thread_rng();
            let n = rnd.gen_range(0..=10000);
            let f = n as f64 / 10000.0;
            if f < self.network_partition {
                let mut rng = thread_rng();
                let ids1 = self.gen_rand_nid_set(&mut rng);
                let ids2 = self.gen_rand_nid_set(&mut rng);
                let ms = self.random_delay_time_ms() * 100;
                vec![
                    FuzzyEvent::PartitionStart(ids1.clone(), ids2.clone()),
                        FuzzyEvent::PartitionRecovery(ms, ids1, ids2)
                ]
            } else {
                vec![]
            }
        }

        fn gen_crash_fuzzy(&self, source:NID, dest:NID) -> Vec<FuzzyEvent> {
            let mut rnd = thread_rng();
            let n = rnd.gen_range(0..=10000);
            let f = n as f64 / 10000.0;
            if f <= self.crash_ratio {

                let m1 = Message::new(
                    RaftMessage::FuzzyTesting::<String>(MFuzzyTesting::Crash), 0, dest);
                let s1 = serde_json::to_string(&m1).unwrap();
                let e1 = Crash(Message::new(s1, source, dest));

                let m2 = Message::new(
                    RaftMessage::FuzzyTesting::<String>(MFuzzyTesting::Restart), 0, dest);
                let s2 = serde_json::to_string(&m2).unwrap();
                let e2 = Restart(self.random_delay_time_ms()*100, Message::new(s2, source, dest));
                vec![e1, e2]
            } else {
                vec![]
            }
        }

        fn gen_message_fuzzy(&self, message:Message<String>) -> Vec<FuzzyEvent> {
            let mut rnd = thread_rng();
            let n = rnd.gen_range(0..=10000);
            let f = n as f64 / 10000.0;
            let mut rnd = thread_rng();
            let mut events = vec![];
            if f <= self.message_delay_ratio {
                let ms = self.random_delay_time_ms();
                events.push(Delay(ms, message.clone()));
            } else if f <= self.message_repeat_ratio +
                self.message_delay_ratio {
                let mut vec = vec![];
                vec.push(0);
                let repeat = rnd.gen_range(1..=self.message_max_repeat);
                for _i in 0..repeat {
                    let ms = self.random_delay_time_ms();
                    vec.push(ms);
                }
                events.push(Duplicate(vec, message.clone()));
            } else if f <= self.message_delay_ratio + self.message_lost_ratio + self.message_lost_ratio {
                events.clear();
                events.push(Lost)
            } else if events.is_empty() {
                events.push(Delay(0, message.clone()))
            }

            events
        }


        fn random_delay_time_ms(&self) -> u64 {
            let mut rnd = thread_rng();
            let millis = self.mean_delayed_millis as f32;
            let normal = Normal::new(
                millis , millis * 0.8f32).unwrap();
            let f = rnd.sample(normal);
            if f < 0.0 {
                return rnd.gen_range(1..self.mean_delayed_millis)
            } else {
                f as u64
            }
        }
    }

    impl FuzzyGenerator for Generator {
        fn gen(&self, cmd: FuzzyCommand) -> Vec<FuzzyEvent> {
            return match cmd {
                FuzzyCommand::Message(m) => {
                    let event = self.gen_message_fuzzy(m.clone());
                    let mut v1 = event;
                    let mut v2 = self.gen_crash_fuzzy(m.source(), m.dest());
                    v1.append(&mut v2);
                    let mut v3 = self.gen_partition_fuzzy();
                    v1.append(&mut v3);
                    v1
                }
            };
        }
    }
    impl <M:MsgTrait + 'static> TestRaftFuzzy<M> {
        pub fn new(
            port_low: u16,
            nodes:u32,
            path:String,
        )  -> Self {

            logger_setup_with_console("debug");
            //set_panic_hook();
            let mut test_nodes = vec![];
            let mut node_peers = vec![];
            let mut server_notifiers = vec![];
            let mut client_notifiers = vec![];
            let mut clients = vec![];
            let mut peers = HashMap::new();
            let fuzzy_server_listen_addr = NodePeer{
                node_id: 100,
                addr:  "0.0.0.0".to_string(),
                port: port_low,
                can_vote: false,
            };
            let fuzzy_server_connect_to_addr = NodePeer{
                node_id: fuzzy_server_listen_addr.node_id,
                addr:  "127.0.0.1".to_string(),
                port: fuzzy_server_listen_addr.port,
                can_vote: fuzzy_server_listen_addr.can_vote,
            };
            let port_base = port_low;
            for i in 1..=nodes {
                let node_id = i as NID;
                let port = port_base + i as u16;
                let node = NodePeer {
                    node_id,
                    addr: "0.0.0.0".to_string(),
                    port,
                    can_vote: true,
                };
                test_nodes.push(node);
                let peer = NodePeer {
                    node_id,
                    addr: "127.0.0.1".to_string(),
                    port,
                    can_vote: true,
                };
                node_peers.push(peer.clone());
                peers.insert(peer.node_id, to_sock_addr(&peer));
                server_notifiers.push(Notifier::new());
                let client_notifier = Notifier::new();
                client_notifiers.push(client_notifier.clone());
                let c = Client::new(
                    node_id + 10, "".to_string(),
                    SocketAddr::from_str(format!("127.0.0.1:{}", port).as_str()).unwrap().to_string(),
                    client_notifier.clone()
                ).unwrap();
                clients.push((c, node_id));
            }

            let handler = EventMessageHandler::new(true);
            let r = handler.opt_receiver().unwrap();
            Self {
                node_db_path: path,
                test_nodes,
                node_peers:node_peers.clone(),
                fuzzy_server_listen: fuzzy_server_listen_addr,
                fuzzy_client_connect_to_server: fuzzy_server_connect_to_addr,
                fuzzy_server_notifier: Default::default(),
                request_notifier: Default::default(),
                server_notifiers,
                client_notifiers,
                clients,
                _peers: peers,
                _nodes: Default::default(),
                receiver: Arc::new(Mutex::new(r)),
                handler: Arc::new(handler),
                event_control: Arc::new(Mutex::new(EventCtrl::new())),
            }
        }

        fn event_cover(&self, event:Message<RaftEvent>) -> bool {
            let mut ctrl = self.event_control.lock().unwrap();
            let all_covered = ctrl.event_cover(event);
            return all_covered;
        }

        fn print_not_covered_event(&self) {
            let ctrl = self.event_control.lock().unwrap();
            ctrl.print_not_covered_event();
        }

        fn run_fuzzy_server(&self) -> Res<JoinHandle<()>> {
            let addr = to_sock_addr(&self.fuzzy_server_listen);
            let j = Self::run_fuzzy_server_inner(
                self.fuzzy_server_listen.node_id.clone(),
                format!("/tmp/fuzzy_server_{}.db", chrono::offset::Utc::now()).to_string(),
                self.fuzzy_server_notifier.clone(),
                addr,
                self._peers.clone()
            )?;
            Ok(j)
        }
        fn run_fuzzy_server_inner(
            nid:NID,
            path:String,
            notifier:Notifier,
            server_addr:SocketAddr,
            peers:HashMap<NID, SocketAddr>,
        ) -> Res<JoinHandle<()>> {
            let mut node_id = vec![];
            for id in peers.keys() {
                node_id.push(id.clone());
            }
            let server = FuzzyServer::new(
                nid,
                "fuzzy_server".to_string(), path,
                notifier, server_addr, peers, Generator::new(node_id))?;
            let thread = thread::Builder::new().spawn(move || {
                server.run();
            }).unwrap();
            Ok(thread)
        }

        pub fn run_raft(&self) -> Res<JoinHandle<()>> {
            let s = self.clone();
            let thread = thread::Builder::new().spawn(move || {
                s.run_all_raft_node(40, 10).unwrap()
            }).unwrap();
            Ok(thread)
        }



        fn run_all_raft_node(&self, ms_tick:u64, timeout_tick:u64) -> Res<() >{
            let mut nodes = vec![];
            for (i, node_peer) in self.test_nodes.iter().enumerate() {
                let name = format!("test_raft_fuzzy_{}", node_peer.node_id);
                let path = PathBuf::from(self.node_db_path.clone());
                if !path.exists() {
                    res_io(fs::create_dir(path.clone()))?;
                }
                let path = path.join(format!("raft_{}.db", name));

                let node = TestRaftNode::start_node::<M>(
                        format!("fuzzy_testing_{}", Uuid::new_v4().to_string()),
                    node_peer.clone(),
                    self.node_peers.clone(),
                    self.server_notifiers[i].clone(),
                    path.to_str().unwrap().to_string(),
                    ms_tick,
                    timeout_tick,
                    4,
                    true,
                    true
                )?;
                nodes.push(node)
            }
            for node in nodes {
                node.join();
            }

            info!("all nodes stopped.");
            Ok(())
        }
    }

    fn _wait_event(test:&TestRaftFuzzy<i32>, bug_inject: Option<BugInject<RaftEvent>>, max_seconds:u64) {
        let inst = Instant::now();
        let incoming = test.receiver.lock().unwrap();
        loop {
            let r = incoming.recv();
            match r {
                Ok(m) => {
                    match &bug_inject {
                        Some(inject) => {
                            let reach = inject.input(m.clone());
                            if reach {
                                info!("inject bugs found");
                                break;
                            }
                        }
                        _ => {}
                    }
                    let finish = test.event_cover(m);
                    if finish {
                        if bug_inject.is_none() {
                            //break;
                        }
                    }
                }
                Err(_e) => {
                    break;
                }
            }
            let duration = inst.elapsed();
            if duration > Duration::from_secs(max_seconds) {
                info!("finish running fuzzy testing after {:?}", duration);
                test.print_not_covered_event();
                break;
            }
        }

        // finish testing
        fuzzy_unset!(RAFT_FUZZY);
        for n in &test.client_notifiers {
            n.notify_all();
        }
        for n in &test.server_notifiers {
            n.notify_all();
        }
        test.fuzzy_server_notifier.notify_all();
        test.request_notifier.notify_all();
    }

    fn _run_raft_test_request(test:&TestRaftFuzzy<i32>) -> Res<()> {
        let notifier = test.request_notifier.clone();
        let mut rnd = thread_rng();
        let ls = LocalSet::new();
        for (c,_) in &test.clients {
            c.run(&ls);
        }

        let clients = test.clients.clone();


        let f = async move {
            for i in 0..i32::MAX {
                sleep(Duration::from_millis(100)).await;
                let opt_c = clients.choose(&mut rnd);
                if let Some(c) = opt_c {
                    let (c, nid) = c.clone();
                    let _ii = i.clone();
                    let (wait_write_local, wait_commit) = if i % 3 == 0 {
                        (false, false)
                    } else if i % 3 == 1 {
                        (true, false)
                    } else {
                        (true, true)
                    };
                    let raft_msg = RaftMessage::ClientReq(
                        MClientReq {
                            id: Uuid::new_v4().to_string(),
                            value: i,
                            source_id: None,
                            wait_write_local,
                            wait_commit,
                            from_client_request: true,
                        }
                    );
                    if !c.is_connected().await {
                        let r = c.connect(OptClientConnect::new()).await;
                        match r {
                            Ok(()) => {}
                            Err(e) => {
                                error!("{}", e);
                            }
                        }
                    }
                    let r = c.send(Message::new(
                        raft_msg,
                        0,
                        nid
                    )).await;
                    match r {
                        Ok(_) => {}
                        Err(_) => {
                            continue;
                        }
                    }
                    select! {
                        r_m = c.recv() => {
                            let _m = r_m?;
                            //debug!("client response {:?}", m);
                        }
                        _ = sleep(Duration::from_millis(100)) => {

                        }
                    }
                    //debug!("client {} request", nid);
                }
            }
            Ok::<(), ET>(())
        };
        ls.spawn_local(async {
            let _ = spawn_local_task(notifier, "", f);
        });
        let runtime = Runtime::new().unwrap();
        runtime.block_on(ls);
        Ok(())
    }

    fn run_raft_request(test:TestRaftFuzzy<i32>) -> Res<JoinHandle<()>> {
        let thread = thread::Builder::new().spawn(move || {
            let r = _run_raft_test_request(&test);
            match r {
                Ok(_) => {}
                Err(e) => { error!("{}", e)}
            }
        }).unwrap();
        Ok(thread)
    }

    fn run_wait(test:TestRaftFuzzy<i32>, bug_inject: Option<BugInject<RaftEvent>>, seconds_max:u64) -> Res<JoinHandle<()>> {
        let thread = thread::Builder::new().spawn(move || {
            _wait_event(&test, bug_inject, seconds_max);
        }).unwrap();
        Ok(thread)
    }

    #[test]
    fn test_raft_fuzzy() {
        run_raft_fuzzy(None, 3, SECONDS_FUZZY_RUN_MAX).unwrap();
    }

    pub fn run_raft_fuzzy(inject_events:Option<String>, nodes:u32, seconds_max:u64) -> Res<()> {
        let _shared = TEST_LOCK.lock().unwrap();
        let opt_bug_inject =
        match inject_events {
            Some(p) => {
                Some(BugInject::<RaftEvent>::from_path(p, vec![])?)
            },
            None => {
                None
            }
        };
        logger_setup("debug");
        let test = TestRaftFuzzy::<i32>::new(
            9111,
            nodes,
            format!("/tmp/test_raft_fuzzy_{}", Uuid::new_v4().to_string())
        );
        event_setup!(RAFT_FUZZY, test.handler.clone());
        let addr = to_sock_addr(&test.fuzzy_client_connect_to_server);
        fuzzy_setup!(RAFT_FUZZY, test.fuzzy_client_connect_to_server.node_id, addr.to_string());


        let fuzzy_server_thd = test.run_fuzzy_server().unwrap();
        let node_run_thread = test.run_raft().unwrap();
        let request_thread = run_raft_request(test.clone()).unwrap();
        let wait_thread = run_wait(test.clone(), opt_bug_inject, seconds_max).unwrap();
        request_thread.join().unwrap();
        node_run_thread.join().unwrap();
        wait_thread.join().unwrap();

        fuzzy_server_thd.join().unwrap();


        Ok(())
    }
}