mod command;
mod connection;
mod error;
mod protocol;
mod server;
mod storage;

use storage::Db;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("myredis=info".parse().unwrap()),
        )
        .init();

    let db = Db::new(16); // 16 shards
    server::run("127.0.0.1:6380", db).await;
}
