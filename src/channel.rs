use scupt_net::message_receiver::ReceiverRR;
use scupt_net::message_sender::{Sender, SenderRR};
use scupt_util::message::MsgTrait;

use crate::raft_message::RaftMessage;

pub trait ChSenderRR <T: MsgTrait + 'static> = SenderRR<RaftMessage<T>>;

pub trait ChSender<T: MsgTrait + 'static> = Sender<RaftMessage<T>>;

pub trait ChReceiver<T: MsgTrait + 'static> = ReceiverRR<RaftMessage<T>>;