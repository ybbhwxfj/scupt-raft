use std::net::{IpAddr, SocketAddr};
use std::process::exit;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use scupt_net::endpoint::Endpoint;
use scupt_net::event_sink::{ESConnectOpt, ESOption, EventSink};
use scupt_net::io_service::{IOService, IOServiceOpt};
use scupt_net::notifier::Notifier;
use scupt_net::task::spawn_local_task;
use scupt_util::message::MsgTrait;
use scupt_util::node_id::NID;
use scupt_util::res::Res;
use scupt_util::res_of::res_parse;
use tokio::runtime::Runtime;
use tokio::task::LocalSet;
use tokio::time::sleep;
use tracing::error;

use crate::raft_config::RaftConf;
use crate::raft_message::RaftMessage;
use crate::sm_store::SMStore;
use crate::state_machine::StateMachine;

type RMsg<T> = RaftMessage<T>;


pub struct SMNode<T: MsgTrait + 'static> {
    conf: RaftConf,
    notify: Notifier,
    service: Arc<IOService<RMsg<T>>>,
    state_machine: Arc<StateMachine<T>>,
}


impl<T: MsgTrait + 'static> SMNode<T> {
    pub fn new(
        conf: RaftConf,
        store: Arc<dyn SMStore<T>>,
        notify: Notifier,
        enable_testing: bool,
    ) -> Res<Self> {
        let opt = IOServiceOpt {
            num_message_receiver: 1,
            testing: enable_testing,
        };
        let service = IOService::<RMsg<T>>::new(
            conf.node_id,
            "RaftService".to_string(),
            opt,
            notify.clone())?;
        let sender = service.default_message_sender();
        let sender_rr = service.default_message_sender_rr();
        let mut receiver_vec = service.message_receiver_rr();
        let receiver = receiver_vec.pop().unwrap();
        let state_machine = StateMachine::new(
            conf.clone(),
            store, sender,
            sender_rr, receiver);
        Ok(SMNode {
            conf,
            notify,
            service: Arc::new(service),
            state_machine: Arc::new(state_machine),
        })
    }

    pub fn run_local(&self, local: &LocalSet) {
        self.start_connect(local);
        self.start_serve(local);
        self.service.run_local(local);
    }

    pub fn run(&self, opt_ls: Option<LocalSet>, runtime: Arc<Runtime>) {
        self.service.run(opt_ls, runtime);
    }

    fn start_serve(&self, local: &LocalSet) {
        let state_machine = self.state_machine.clone();
        let notify = self.notify.clone();
        let event = self.service.default_event_sink().clone();
        let conf = self.conf.clone();
        let id = conf.node_id;
        local.spawn_local(async move {
            spawn_local_task(notify, format!("state_machine_{}_serve", id).as_str(),
                             async move {
                                 let r = Self::serve(conf, state_machine, event).await;
                                 match r {
                                     Ok(_) => {}
                                     Err(e) => {
                                         error!("{}", e.to_string());
                                         exit(-1);
                                     }
                                 }
                             },
            ).unwrap();
        });
    }

    async fn serve(
        conf: RaftConf,
        state_machine: Arc<StateMachine<T>>,
        event: Arc<dyn EventSink>,
    ) -> Res<()> {
        state_machine.recovery().await?;
        let ip_addr = res_parse(IpAddr::from_str(conf.bind_address.as_str()))?;
        let sock_addr = SocketAddr::new(ip_addr, conf.bind_port);
        event.serve(sock_addr, ESOption::default()).await?;
        state_machine.serve_loop().await?;
        Ok(())
    }

    fn start_connect(&self, local: &LocalSet) {
        for node_peer in self.conf.node_peer.iter() {
            if node_peer.node_id != self.conf.node_id {
                let nid = node_peer.node_id.clone();
                let addr = SocketAddr::new(IpAddr::from_str(node_peer.addr.as_str()).unwrap(), node_peer.port);
                let notify = self.notify.clone();
                let event_sink = self.service.default_event_sink().clone();
                local.spawn_local(
                    async move {
                        spawn_local_task(notify,
                                         "connect_to_raft_peer",
                                         async move {
                                             let r = Self::start_connect_to(event_sink, nid, addr).await;
                                             match r {
                                                 Ok(_) => {}
                                                 Err(e) => { panic!("{}", e); }
                                             }
                                         },
                        ).unwrap()
                    }
                );
            }
        }
    }

    async fn start_connect_to(event_sink: Arc<dyn EventSink>, nid: NID, addr: SocketAddr) -> Res<Endpoint> {
        loop {
            let r = event_sink.connect(
                nid,
                addr,
                ESConnectOpt::default().enable_return_endpoint(true),
            ).await;
            match r {
                Ok(opt_ep) => {
                    return Ok(opt_ep.unwrap());
                }
                Err(_e) => {
                    sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }
}