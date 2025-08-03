use std::collections::{HashMap, HashSet};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use crate::config::ServerConfig;
use crate::rdb::Store;

pub type Replicas = Arc<Mutex<HashMap<std::net::SocketAddr, (TcpStream, usize)>>>;
pub type BlockingList = Arc<Mutex<HashMap<String, Vec<TcpStream>>>>;

pub struct Context {
    // global state
    pub cfg:       Arc<ServerConfig>,
    pub store:     Arc<Store>,
    pub replicas:  Replicas,
    pub blocking:  BlockingList,
    pub master_repl_offset: usize,
    pub pending_writes: Arc<Mutex<Vec<Vec<String>>>>,

    // pub/sub registry: channel → list of subscribers
    pub pubsub:   Arc<Mutex<HashMap<String, Vec<TcpStream>>>>,

    // per‐connection state
    pub in_transaction: bool,
    pub queued: Vec<(String, Vec<String>)>,
    pub this_client: Option<TcpStream>,

    // which channels *this* client is on
    pub subscribed_channels: HashSet<String>,
}

impl Clone for Context {
    fn clone(&self) -> Self {
        println!(
            "[Context::clone] Cloning Context (repl_offset={}, tx_mode={}, queued_cmds={})",
            self.master_repl_offset, self.in_transaction, self.queued.len()
        );
        Self {
            cfg:                   self.cfg.clone(),
            store:                 self.store.clone(),
            replicas:              self.replicas.clone(),
            blocking:              self.blocking.clone(),
            master_repl_offset:    self.master_repl_offset,
            pending_writes:        self.pending_writes.clone(),

            pubsub:               self.pubsub.clone(),

            in_transaction:        self.in_transaction,
            queued:                self.queued.clone(),
            this_client:          self.this_client.as_ref().and_then(|s| s.try_clone().ok()),

            subscribed_channels:  self.subscribed_channels.clone(),
        }
    }
}
