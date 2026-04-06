use tokio::net::TcpListener;
use tracing::{error, info};

use crate::connection::Connection;
use crate::storage::Db;

pub async fn run(addr: &str, db: Db) {
    let listener = TcpListener::bind(addr)
        .await
        .expect("failed to bind address");
    run_listener(listener, db).await;
}

pub async fn run_listener(listener: TcpListener, db: Db) {
    if let Ok(addr) = listener.local_addr() {
        info!("myredis listening on {}", addr);
    }

    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                if let Err(e) = stream.set_nodelay(true) {
                    error!("failed to set TCP_NODELAY for {}: {}", peer, e);
                }
                info!("new connection from {}", peer);
                let db = db.clone();
                tokio::spawn(async move {
                    Connection::new(stream, db).run().await;
                    info!("connection closed: {}", peer);
                });
            }
            Err(e) => error!("accept error: {}", e),
        }
    }
}
