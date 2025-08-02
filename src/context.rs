use std::collections::HashMap;
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use crate::config::ServerConfig;
use crate::rdb::Store;

pub type BlockingList = Arc<Mutex<HashMap<String, Vec<TcpStream>>>>;

/// Holds *both* the global server state (all Arcs)
/// and the per‐connection transaction state (plain fields).
pub struct Context {
    // global:
    pub cfg:       Arc<ServerConfig>,
    pub store:     Arc<Store>,
    pub replicas:  Arc<Mutex<Vec<TcpStream>>>,
    pub blocking:  BlockingList,
    pub master_repl_offset: usize,

    // per‐connection:
    pub in_transaction: bool,                       // tracks MULTI…EXEC
    pub queued: Vec<(String, Vec<String>)>,         // buffers (cmd, args)
    pub this_client: Option<TcpStream>,
}

impl Clone for Context {
    fn clone(&self) -> Self {
        Self {
            cfg: self.cfg.clone(),
            store: self.store.clone(),
            replicas: self.replicas.clone(),
            blocking: self.blocking.clone(),
            master_repl_offset: self.master_repl_offset,
            in_transaction: self.in_transaction,
            queued: self.queued.clone(),
            this_client: self.this_client.as_ref().and_then(|s| s.try_clone().ok()),
        }
    }
}
