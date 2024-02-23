#[cfg(test)]
pub mod tests {
    use std::{fs, thread};
    use std::collections::HashMap;
    use std::net::{IpAddr, SocketAddr};
    use std::path::PathBuf;
    use std::str::FromStr;
    use std::sync::{Arc, Condvar, Mutex};
    use std::thread::sleep;
    use std::time::Duration;

    use scupt_net::notifier::Notifier;
    use scupt_util::logger::logger_setup;
    use scupt_util::node_id::NID;
    use scupt_util::res::Res;
    use scupt_util::res_of::res_io;
    use sedeve_kit::{auto_clear, auto_init};
    use sedeve_kit::action::panic::set_panic_hook;
    use sedeve_kit::player::action_incoming::ActionIncoming;
    use sedeve_kit::player::action_incoming_factory::ActionIncomingFactory;
    use sedeve_kit::player::automata;
    use sedeve_kit::player::dtm_player::{DTMPlayer, TestOption};
    use sedeve_kit::trace_gen::trace_reader::TraceReader;
    use tracing::info;
    use uuid::Uuid;

    use crate::raft_config::NodePeer;
    use crate::raft_message::RAFT_ABSTRACT;
    use crate::test_config::tests::TEST_LOCK;
    use crate::test_path::tests::test_data_path;
    use crate::test_raft_node::tests::TestRaftNode;

    #[derive(Debug)]
    pub enum InputType {
        FromDB(String),
        FromJsonFile(String),
    }

    #[derive(Clone)]
    pub struct StopSink {
        state:Arc<Mutex<u32>>,
        cond:Arc<Condvar>,
    }
    const WAITING:u32 = 0u32;
    const STOPPED:u32 = 1u32;
    const NOTIFIED:u32 = 2u32;
    impl StopSink {
        pub fn new() -> Self {
            let mutex = Mutex::new(WAITING);
            Self {
                state: Arc::new(mutex),
                cond:Arc::new(Condvar::new())
            }
        }
        pub fn stop(&self) {
            let mut stopped = self.state.lock().unwrap();
            if *stopped != STOPPED {
                *stopped = STOPPED;
                self.cond.notify_all();
            }
        }

        pub fn notify(&self) {
            let mut stopped = self.state.lock().unwrap();
            if *stopped != STOPPED {
                *stopped = NOTIFIED;
                self.cond.notify_all();
            }
        }
        pub fn is_stopped(&self) -> bool {
            let stopped = self.state.lock().unwrap();
            *stopped == 1
        }

        pub fn wait(&self) {
            let mut stopped = self.state.lock().unwrap();
            while *stopped == WAITING {
                stopped = self.cond.wait(stopped).unwrap();
            }
            if *stopped == NOTIFIED {
                *stopped = WAITING;
            }
        }
    }
    #[derive(Clone)]
    struct TestRaft {
        opt_stop_sink:Option<StopSink>,
        node_db_path: String,
        simulator_address: SocketAddr,
        simulator_node_id: NID,
        test_nodes: Vec<NodePeer>,
        node_peers: Vec<NodePeer>,
        peers: HashMap<NID, SocketAddr>,
        player_stop_notifier : Notifier,
        node_stop_notifier:Vec<Notifier>,
    }

    impl TestRaft {
        pub fn test_db_input_case(
            trace_db_path: String,
            node_db_path: String,
            simulator: (NID, SocketAddr),
            test_nodes: Vec<NodePeer>,
            node_peers: Vec<NodePeer>,
            auto_name: String,
            stop_sink:Option<StopSink>
        ) -> Res<()> {
            let simulator_id = simulator.0.clone();
            let simulator_addr = simulator.1.clone();
            let t = Self::create(node_db_path, simulator_id, simulator_addr, test_nodes, node_peers, stop_sink)?;
            t.run_test_from_db(trace_db_path, auto_name.as_str())
        }

        pub fn stop(&self) {
            if let Some(s) = &self.opt_stop_sink {
                if !s.is_stopped() {
                    s.notify();
                }
            }
            sleep(Duration::from_millis(50));
            for i in self.node_stop_notifier.iter() {
                i.notify_all();
            }
            sleep(Duration::from_millis(50));
            self.player_stop_notifier.notify_all();
        }

        pub fn test_file_input_case(
            trace_file: String,
            node_db_path: String,
            simulator: (NID, SocketAddr),
            test_nodes: Vec<NodePeer>,
            node_peers: Vec<NodePeer>,
            auto_name: String,
            stop_sink:Option<StopSink>
        ) -> Res<()> {
            let simulator_id = simulator.0.clone();
            let simulator_addr = simulator.1.clone();
            let t = Self::create(node_db_path, simulator_id, simulator_addr, test_nodes, node_peers, stop_sink)?;
            let i = ActionIncomingFactory::action_incoming_from_json_file(trace_file)?;
            t.run_case(i, auto_name.as_str())
        }


        fn create(
            node_db_path: String,
            simulator_id: NID,
            simulator_addr: SocketAddr,
            test_nodes: Vec<NodePeer>,
            node_peers: Vec<NodePeer>,
            stop_sink:Option<StopSink>,
        ) -> Res<Self> {
            let mut map_test_node = HashMap::new();
            for p in node_peers.iter() {
                let ip_addr = IpAddr::from_str(p.addr.as_str()).unwrap();
                let sock_addr = SocketAddr::new(ip_addr, p.port);
                map_test_node.insert(p.node_id, sock_addr);
            }
            let mut node_notifier = vec![];
            for _i in 0..test_nodes.len() {
                node_notifier.push(Notifier::new());
            }

            let t = Self {
                opt_stop_sink:stop_sink.clone(),
                node_db_path,
                simulator_address: simulator_addr,
                simulator_node_id: simulator_id,
                test_nodes,
                node_peers,
                peers: map_test_node,
                player_stop_notifier: Default::default(),
                node_stop_notifier:node_notifier,
            };
            if stop_sink.is_some() {
                t.stop_thread_run();
            }
            Ok(t)
        }

        fn run_test_from_db(&self, db_path: String, test_auto_name: &str) -> Res<()> {
            let vec_incoming =
                TraceReader::read_trace(db_path)?;
            for (n, p) in vec_incoming.iter().enumerate() {
                if let Some(s) = &self.opt_stop_sink {
                    if s.is_stopped() {
                        return Ok(())
                    }
                }
                info!("run test {} case {} ", test_auto_name, n + 1);
                self.run_case(p.clone(), test_auto_name)?;
            }
            Ok(())
        }

        fn stop_thread_run(&self) {
            let test = self.clone();
            let _ = thread::Builder::new().spawn(move || {
                if let Some(s) = & test.opt_stop_sink {
                    s.wait();
                    test.stop();
                }
            });
        }
        fn run_case(
            &self,
            p: Arc<dyn ActionIncoming>,
            test_auto_name: &str,
        ) -> Res<()> {

            let id = self.simulator_node_id.clone();
            let addr = self.simulator_address.clone();
            let peer = self.peers.clone();
            let notifier_for_run_player = self.player_stop_notifier.clone();

            let simulator_node_id = self.simulator_node_id;
            let simulate_server = self.simulator_address;
            let addr_str = simulate_server.to_string();
            auto_init!(
                    test_auto_name,
                    0,
                    simulator_node_id,
                    addr_str.as_str()
                );
            let incoming = p.clone();
            let opt = if !test_auto_name.eq(&RAFT_ABSTRACT.to_string()) {
                TestOption::new()
                    .set_wait_both_begin_and_end_action(false)
                    .set_sequential_output_action(false)
            } else {
                TestOption::new()
                    .set_wait_both_begin_and_end_action(true)
                    .set_sequential_output_action(false)
            };
            let test = self.clone();
            let f_done = move || {
                test.stop();
            };
            let t = move || {
                DTMPlayer::run_trace(
                    id,
                    addr,
                    peer,
                    incoming.clone(),
                    notifier_for_run_player.clone(),
                    opt,
                    f_done,
                )
            };
            let thread = thread::Builder::new()
                .name("dtm_run_trace".to_string())
                .spawn(t).unwrap();

            let mut nodes = vec![];
            for (i, node_peer) in self.test_nodes.iter().enumerate() {
                let name = format!("test_{}", node_peer.node_id);
                let path = PathBuf::from(self.node_db_path.clone());
                if !path.exists() {
                    res_io(fs::create_dir(path.clone()))?;
                }
                let path = path.join(format!("raft_{}.db", name));
                let node = TestRaftNode::start_node::<i32>(
                    format!("dtm_{}_{}", test_auto_name, Uuid::new_v4().to_string()),
                    node_peer.clone(),
                    self.node_peers.clone(),
                    self.node_stop_notifier[i].clone(),
                    path.to_str().unwrap().to_string(),
                    0,
                    0,
                    1,
                    true,
                    false
                )?;
                nodes.push(node)
            }

            let _ = thread.join().unwrap();
            for node in nodes {
                node.join();
            }

            auto_clear!(test_auto_name);
            Ok(())
        }
    }


    pub fn test_raft_gut(
        from_db: InputType,
        port_low: u16,
        num_nodes:u32,
        auto_name: String,
        opt_stop_sink:Option<StopSink>,
    ) {
        set_panic_hook();
        let _shared = TEST_LOCK.lock().unwrap();
        logger_setup("debug");
        info!("raft deterministic testing {:?}", from_db);
        let mut test_nodes = vec![];
        let mut test_peers = vec![];
        let port_base = port_low;
        let simulator_node = (
            1000 as NID,
            SocketAddr::new(IpAddr::V4("127.0.0.1".parse().unwrap()), port_base)
        );
        for i in 1..=num_nodes as u16 {
            let node_id = i as NID;
            let port = port_base + i;
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
            test_peers.push(peer);
        }


        let db_path = PathBuf::new().join("/tmp".to_string()).join(
            format!("test_raft_dtm_{}", Uuid::new_v4().to_string()));
        let node_db_path = db_path.to_str().unwrap().to_string();
        match from_db {
            InputType::FromDB(p) => {
                let path = test_data_path(p).unwrap();
                TestRaft::test_db_input_case(
                    path,
                    node_db_path,
                    simulator_node, test_nodes, test_peers, auto_name, opt_stop_sink).unwrap();
            }
            InputType::FromJsonFile(p) => {
                let path = test_data_path(p).unwrap();
                TestRaft::test_file_input_case(
                    path,
                    node_db_path,
                    simulator_node, test_nodes, test_peers, auto_name, opt_stop_sink).unwrap();
            }
        }
    }
}
