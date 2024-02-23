

#[cfg(test)]
pub(crate) mod tests {
    use scupt_net::notifier::Notifier;
    use scupt_util::res::Res;
    use std::thread;
    use tracing::{error,  trace};
    use std::process::exit;
    use std::sync::Arc;
    use scupt_util::error_type::ET;
    use scupt_util::message::MsgTrait;
    use tokio::task::LocalSet;
    use tokio::runtime::Builder;
    use crate::raft_config::{NodePeer, RaftConf};
    use crate::sm_node::SMNode;
    use crate::test_store_simple::tests::SMStoreSimple;

    /// portal of raft testing
    /// including specification-driven testing and fuzzy testing
    pub struct TestRaftNode {
        join_handle: thread::JoinHandle<()>,
    }

    impl TestRaftNode {
        pub fn start_node<M:MsgTrait + 'static>(
            cluster_name:String,
            this_peer: NodePeer,
            peers: Vec<NodePeer>,
            notifier: Notifier,
            storage_path: String,
            ms_tick:u64,
            timeout_max_tick:u64,
            max_compact_entries:u64,
            enable_check_invariants:bool,
            send_to_leader:bool,
        ) -> Res<TestRaftNode> {
            let mut conf = RaftConf::new(
                cluster_name,
                this_peer.node_id,
                storage_path.clone(),
                this_peer.addr.clone(), this_peer.port);
            conf.ms_tick = ms_tick;
            conf.timeout_max_tick = timeout_max_tick;
            conf.max_compact_entries = max_compact_entries;
            conf.send_value_to_leader = send_to_leader;
            for peer in peers.iter() {
                conf.add_peers(peer.node_id, peer.addr.clone(), peer.port, peer.can_vote);
            }

            let r_join_handle = thread::Builder::new()
                .name(format!("node_raft_{}", this_peer.node_id))
                .spawn(move || {
                    let r = Self::thread_run::<M>(conf, notifier, enable_check_invariants);
                    match r {
                        Ok(()) => {}
                        Err(e) => {
                            error!("{}", e.to_string());
                            exit(-1);
                        }
                    }
                    trace!("node {} stop", this_peer.node_id);
                });
            let join_handle = match r_join_handle {
                Ok(join_handle) => { join_handle }
                Err(e) => {
                    error!("spawn thread error, {}", e);
                    return Err(ET::IOError(e.to_string()));
                }
            };
            let n = TestRaftNode {
                join_handle,
            };
            Ok(n)
        }

        pub fn thread_run<M:MsgTrait + 'static>(
            conf: RaftConf,
            notifier: Notifier,
            enable_check_invariants:bool
        ) -> Res<()> {
            let store = Arc::new(SMStoreSimple::create(
                conf.clone(),
                enable_check_invariants
            )?);
            let node = SMNode::<M>::new(conf, store, notifier, true)?;
            let ls = LocalSet::new();
            let r = Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let runtime = Arc::new(r);
            node.run_local(&ls);
            node.run(Some(ls), runtime);
            Ok(())
        }
        pub fn join(self) {
            self.join_handle.join().unwrap();
        }
    }
}

