use std::net::SocketAddr;

use bytes::{Bytes, BytesMut};
use myredis::protocol::{parse_frame, Frame};
use myredis::server;
use myredis::storage::Db;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};

struct TestServer {
    addr: SocketAddr,
    task: JoinHandle<()>,
}

impl TestServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("failed to bind ephemeral port");
        let addr = listener.local_addr().expect("failed to read local addr");
        let db = Db::new(16);
        let task = tokio::spawn(async move {
            server::run_listener(listener, db).await;
        });

        // Give accept loop a chance to start.
        for _ in 0..50 {
            if TcpStream::connect(addr).await.is_ok() {
                break;
            }
            tokio::task::yield_now().await;
        }

        Self { addr, task }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct TestClient {
    stream: TcpStream,
    read_buf: BytesMut,
}

impl TestClient {
    async fn connect(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr)
            .await
            .expect("failed to connect test client");
        Self {
            stream,
            read_buf: BytesMut::with_capacity(1024),
        }
    }

    async fn send_raw(&mut self, data: &[u8]) {
        self.stream
            .write_all(data)
            .await
            .expect("failed to write request");
    }

    async fn send_cmd(&mut self, args: &[&str]) -> Frame {
        self.send_raw(&encode_cmd(args)).await;
        self.read_frame().await
    }

    async fn read_frame(&mut self) -> Frame {
        loop {
            match parse_frame(&mut self.read_buf).expect("response parse failed") {
                Some(frame) => return frame,
                None => {
                    let n = self
                        .stream
                        .read_buf(&mut self.read_buf)
                        .await
                        .expect("failed to read response");
                    assert!(n > 0, "connection closed before a complete response frame");
                }
            }
        }
    }
}

fn encode_cmd(args: &[&str]) -> Vec<u8> {
    let items = args
        .iter()
        .map(|arg| Frame::BulkString(Bytes::copy_from_slice(arg.as_bytes())))
        .collect();
    Frame::Array(items).encode()
}

fn encode_cmd_bytes(args: &[&[u8]]) -> Vec<u8> {
    let items = args
        .iter()
        .map(|arg| Frame::BulkString(Bytes::copy_from_slice(arg)))
        .collect();
    Frame::Array(items).encode()
}

#[tokio::test]
async fn multi_connection_shared_state() {
    let server = TestServer::start().await;
    let mut c1 = TestClient::connect(server.addr).await;
    let mut c2 = TestClient::connect(server.addr).await;

    let set_resp = c1.send_cmd(&["SET", "shared:key", "value-1"]).await;
    assert_eq!(set_resp, Frame::SimpleString("OK".to_string()));

    let get_resp = c2.send_cmd(&["GET", "shared:key"]).await;
    assert_eq!(get_resp, Frame::BulkString(Bytes::from_static(b"value-1")));
}

#[tokio::test]
async fn pipeline_response_order_is_consistent() {
    let server = TestServer::start().await;
    let mut c = TestClient::connect(server.addr).await;

    let mut pipeline = Vec::new();
    pipeline.extend_from_slice(&encode_cmd(&["SET", "pipe:key", "v1"]));
    pipeline.extend_from_slice(&encode_cmd(&["GET", "pipe:key"]));
    pipeline.extend_from_slice(&encode_cmd(&["DEL", "pipe:key"]));
    c.send_raw(&pipeline).await;

    let r1 = c.read_frame().await;
    let r2 = c.read_frame().await;
    let r3 = c.read_frame().await;

    assert_eq!(r1, Frame::SimpleString("OK".to_string()));
    assert_eq!(r2, Frame::BulkString(Bytes::from_static(b"v1")));
    assert_eq!(r3, Frame::Integer(1));
}

#[tokio::test]
async fn ping_with_message_returns_bulk_string() {
    let server = TestServer::start().await;
    let mut c = TestClient::connect(server.addr).await;

    let resp = c.send_cmd(&["PING", "hello"]).await;
    assert_eq!(resp, Frame::BulkString(Bytes::from_static(b"hello")));
}

#[tokio::test]
async fn wrong_argument_count_returns_error() {
    let server = TestServer::start().await;
    let mut c = TestClient::connect(server.addr).await;

    let resp = c.send_cmd(&["GET", "foo", "bar"]).await;
    match resp {
        Frame::Error(message) => {
            assert!(message.contains("wrong number of arguments for 'GET'"));
        }
        other => panic!("expected error response, got {:?}", other),
    }
}

#[tokio::test]
async fn basic_string_commands_end_to_end() {
    let server = TestServer::start().await;
    let mut c = TestClient::connect(server.addr).await;

    assert_eq!(
        c.send_cmd(&["SET", "k1", "v1"]).await,
        Frame::SimpleString("OK".to_string())
    );
    assert_eq!(
        c.send_cmd(&["GET", "k1"]).await,
        Frame::BulkString(Bytes::from_static(b"v1"))
    );
    assert_eq!(c.send_cmd(&["EXISTS", "k1"]).await, Frame::Integer(1));
    assert_eq!(c.send_cmd(&["DEL", "k1"]).await, Frame::Integer(1));
    assert_eq!(c.send_cmd(&["EXISTS", "k1"]).await, Frame::Integer(0));
}

#[tokio::test]
async fn hash_commands_end_to_end() {
    let server = TestServer::start().await;
    let mut c = TestClient::connect(server.addr).await;

    assert_eq!(
        c.send_cmd(&["HSET", "h", "f1", "v1", "f2", "v2"]).await,
        Frame::Integer(2)
    );
    assert_eq!(
        c.send_cmd(&["HGET", "h", "f1"]).await,
        Frame::BulkString(Bytes::from_static(b"v1"))
    );
    assert_eq!(c.send_cmd(&["HLEN", "h"]).await, Frame::Integer(2));
    assert_eq!(c.send_cmd(&["HDEL", "h", "f1"]).await, Frame::Integer(1));
    assert_eq!(c.send_cmd(&["HGET", "h", "f1"]).await, Frame::Null);
}

#[tokio::test]
async fn list_commands_end_to_end() {
    let server = TestServer::start().await;
    let mut c = TestClient::connect(server.addr).await;

    assert_eq!(
        c.send_cmd(&["LPUSH", "list", "a", "b"]).await,
        Frame::Integer(2)
    );
    assert_eq!(c.send_cmd(&["RPUSH", "list", "c"]).await, Frame::Integer(3));
    assert_eq!(c.send_cmd(&["LLEN", "list"]).await, Frame::Integer(3));
    assert_eq!(
        c.send_cmd(&["LPOP", "list"]).await,
        Frame::BulkString(Bytes::from_static(b"b"))
    );
    assert_eq!(
        c.send_cmd(&["RPOP", "list"]).await,
        Frame::BulkString(Bytes::from_static(b"c"))
    );
    assert_eq!(c.send_cmd(&["LLEN", "list"]).await, Frame::Integer(1));
}

#[tokio::test]
async fn expire_and_ttl_basic_behavior() {
    let server = TestServer::start().await;
    let mut c = TestClient::connect(server.addr).await;

    assert_eq!(
        c.send_cmd(&["SET", "temp", "1"]).await,
        Frame::SimpleString("OK".to_string())
    );
    assert_eq!(
        c.send_cmd(&["EXPIRE", "temp", "1"]).await,
        Frame::Integer(1)
    );

    let ttl_now = c.send_cmd(&["TTL", "temp"]).await;
    match ttl_now {
        Frame::Integer(v) => assert!((0..=1).contains(&v)),
        other => panic!("expected integer ttl, got {:?}", other),
    }

    sleep(Duration::from_millis(1200)).await;

    assert_eq!(c.send_cmd(&["TTL", "temp"]).await, Frame::Integer(-2));
    assert_eq!(c.send_cmd(&["GET", "temp"]).await, Frame::Null);
}

#[tokio::test]
async fn wrongtype_error_for_get_on_hash_key() {
    let server = TestServer::start().await;
    let mut c = TestClient::connect(server.addr).await;

    assert_eq!(
        c.send_cmd(&["HSET", "obj", "field", "v"]).await,
        Frame::Integer(1)
    );

    let resp = c.send_cmd(&["GET", "obj"]).await;
    match resp {
        Frame::Error(message) => {
            assert!(message.contains("WRONGTYPE"));
        }
        other => panic!("expected WRONGTYPE error, got {:?}", other),
    }
}

#[tokio::test]
async fn binary_key_value() {
    let server = TestServer::start().await;
    let mut c = TestClient::connect(server.addr).await;

    let key = b"k\r\n\0x";
    let value = b"v\r\n\0y";

    c.send_raw(&encode_cmd_bytes(&[b"SET", key, value])).await;
    assert_eq!(c.read_frame().await, Frame::SimpleString("OK".to_string()));

    c.send_raw(&encode_cmd_bytes(&[b"GET", key])).await;
    assert_eq!(
        c.read_frame().await,
        Frame::BulkString(Bytes::copy_from_slice(value))
    );
}

#[tokio::test]
async fn empty_key_and_value() {
    let server = TestServer::start().await;
    let mut c = TestClient::connect(server.addr).await;

    assert_eq!(
        c.send_cmd(&["SET", "", ""]).await,
        Frame::SimpleString("OK".to_string())
    );
    assert_eq!(
        c.send_cmd(&["GET", ""]).await,
        Frame::BulkString(Bytes::from_static(b""))
    );
}

#[tokio::test]
async fn pipeline_with_errors() {
    let server = TestServer::start().await;
    let mut c = TestClient::connect(server.addr).await;

    let mut pipeline = Vec::new();
    pipeline.extend_from_slice(&encode_cmd(&["SET", "p:err", "ok"]));
    pipeline.extend_from_slice(&encode_cmd(&["GET", "p:err", "extra"]));
    pipeline.extend_from_slice(&encode_cmd(&["GET", "p:err"]));
    c.send_raw(&pipeline).await;

    assert_eq!(c.read_frame().await, Frame::SimpleString("OK".to_string()));
    match c.read_frame().await {
        Frame::Error(message) => assert!(message.contains("wrong number of arguments for 'GET'")),
        other => panic!("expected arg error, got {:?}", other),
    }
    assert_eq!(
        c.read_frame().await,
        Frame::BulkString(Bytes::from_static(b"ok"))
    );
}

#[tokio::test]
async fn hexists_not_implemented_graceful() {
    let server = TestServer::start().await;
    let mut c = TestClient::connect(server.addr).await;

    let resp = c.send_cmd(&["HEXISTS", "h", "f"]).await;
    match resp {
        Frame::Error(message) => {
            assert!(message.contains("unknown command 'HEXISTS'"));
        }
        other => panic!("expected unknown command error, got {:?}", other),
    }
}

#[tokio::test]
async fn incr_command_behavior() {
    let server = TestServer::start().await;
    let mut c = TestClient::connect(server.addr).await;

    assert_eq!(c.send_cmd(&["INCR", "counter"]).await, Frame::Integer(1));
    assert_eq!(c.send_cmd(&["INCR", "counter"]).await, Frame::Integer(2));

    assert_eq!(
        c.send_cmd(&["SET", "not-int", "abc"]).await,
        Frame::SimpleString("OK".to_string())
    );
    match c.send_cmd(&["INCR", "not-int"]).await {
        Frame::Error(message) => assert!(message.contains("value is not an integer")),
        other => panic!("expected integer conversion error, got {:?}", other),
    }
}

#[tokio::test]
async fn flushall_and_dbsize() {
    let server = TestServer::start().await;
    let mut c = TestClient::connect(server.addr).await;

    assert_eq!(
        c.send_cmd(&["SET", "k1", "v1"]).await,
        Frame::SimpleString("OK".to_string())
    );
    assert_eq!(
        c.send_cmd(&["SET", "k2", "v2"]).await,
        Frame::SimpleString("OK".to_string())
    );
    assert_eq!(c.send_cmd(&["DBSIZE"]).await, Frame::Integer(2));
    assert_eq!(
        c.send_cmd(&["FLUSHALL"]).await,
        Frame::SimpleString("OK".to_string())
    );
    assert_eq!(c.send_cmd(&["DBSIZE"]).await, Frame::Integer(0));
}

#[tokio::test]
async fn ttl_on_nonexistent_key() {
    let server = TestServer::start().await;
    let mut c = TestClient::connect(server.addr).await;

    assert_eq!(
        c.send_cmd(&["TTL", "no-such-key"]).await,
        Frame::Integer(-2)
    );
}

#[tokio::test]
async fn del_multiple_keys() {
    let server = TestServer::start().await;
    let mut c = TestClient::connect(server.addr).await;

    assert_eq!(
        c.send_cmd(&["SET", "k1", "v1"]).await,
        Frame::SimpleString("OK".to_string())
    );
    assert_eq!(
        c.send_cmd(&["SET", "k2", "v2"]).await,
        Frame::SimpleString("OK".to_string())
    );

    assert_eq!(
        c.send_cmd(&["DEL", "k1", "k2", "k3"]).await,
        Frame::Integer(2)
    );
    assert_eq!(c.send_cmd(&["EXISTS", "k1", "k2"]).await, Frame::Integer(0));
}

#[tokio::test]
async fn exists_multiple_keys() {
    let server = TestServer::start().await;
    let mut c = TestClient::connect(server.addr).await;

    assert_eq!(
        c.send_cmd(&["SET", "k1", "v1"]).await,
        Frame::SimpleString("OK".to_string())
    );
    assert_eq!(
        c.send_cmd(&["SET", "k3", "v3"]).await,
        Frame::SimpleString("OK".to_string())
    );
    assert_eq!(
        c.send_cmd(&["EXISTS", "k1", "k2", "k3"]).await,
        Frame::Integer(2)
    );
}

#[tokio::test]
async fn incr_on_string() {
    let server = TestServer::start().await;
    let mut c = TestClient::connect(server.addr).await;

    assert_eq!(c.send_cmd(&["INCR", "counter"]).await, Frame::Integer(1));
    assert_eq!(c.send_cmd(&["INCR", "counter"]).await, Frame::Integer(2));
    assert_eq!(
        c.send_cmd(&["GET", "counter"]).await,
        Frame::BulkString(Bytes::from_static(b"2"))
    );
}

#[tokio::test]
async fn incr_on_non_integer() {
    let server = TestServer::start().await;
    let mut c = TestClient::connect(server.addr).await;

    assert_eq!(
        c.send_cmd(&["SET", "bad-counter", "abc"]).await,
        Frame::SimpleString("OK".to_string())
    );
    match c.send_cmd(&["INCR", "bad-counter"]).await {
        Frame::Error(message) => assert!(message.contains("value is not an integer")),
        other => panic!("expected integer conversion error, got {:?}", other),
    }
}

#[tokio::test]
async fn flushall_then_dbsize() {
    let server = TestServer::start().await;
    let mut c = TestClient::connect(server.addr).await;

    assert_eq!(
        c.send_cmd(&["SET", "a", "1"]).await,
        Frame::SimpleString("OK".to_string())
    );
    assert_eq!(
        c.send_cmd(&["HSET", "h", "f", "v"]).await,
        Frame::Integer(1)
    );
    assert_eq!(c.send_cmd(&["LPUSH", "l", "x"]).await, Frame::Integer(1));
    assert_eq!(c.send_cmd(&["DBSIZE"]).await, Frame::Integer(3));

    assert_eq!(
        c.send_cmd(&["FLUSHALL"]).await,
        Frame::SimpleString("OK".to_string())
    );
    assert_eq!(c.send_cmd(&["DBSIZE"]).await, Frame::Integer(0));
}

#[tokio::test]
async fn hlen_on_non_hash() {
    let server = TestServer::start().await;
    let mut c = TestClient::connect(server.addr).await;

    assert_eq!(
        c.send_cmd(&["SET", "plain", "v"]).await,
        Frame::SimpleString("OK".to_string())
    );
    match c.send_cmd(&["HLEN", "plain"]).await {
        Frame::Error(message) => assert!(message.contains("WRONGTYPE")),
        other => panic!("expected WRONGTYPE, got {:?}", other),
    }
}

#[tokio::test]
async fn llen_on_non_list() {
    let server = TestServer::start().await;
    let mut c = TestClient::connect(server.addr).await;

    assert_eq!(
        c.send_cmd(&["SET", "plain", "v"]).await,
        Frame::SimpleString("OK".to_string())
    );
    match c.send_cmd(&["LLEN", "plain"]).await {
        Frame::Error(message) => assert!(message.contains("WRONGTYPE")),
        other => panic!("expected WRONGTYPE, got {:?}", other),
    }
}

#[tokio::test]
async fn expire_on_nonexistent_key() {
    let server = TestServer::start().await;
    let mut c = TestClient::connect(server.addr).await;

    assert_eq!(
        c.send_cmd(&["EXPIRE", "never", "5"]).await,
        Frame::Integer(0)
    );
}

#[tokio::test]
async fn ttl_persistence() {
    let server = TestServer::start().await;
    let mut c = TestClient::connect(server.addr).await;

    let resp = c.send_cmd(&["PERSIST", "k1"]).await;
    match resp {
        Frame::Error(message) => {
            assert!(message.contains("unknown command 'PERSIST'"));
        }
        other => panic!("expected unknown command error, got {:?}", other),
    }
}

#[tokio::test]
async fn type_command() {
    let server = TestServer::start().await;
    let mut c = TestClient::connect(server.addr).await;

    assert_eq!(
        c.send_cmd(&["TYPE", "missing"]).await,
        Frame::SimpleString("none".to_string())
    );

    assert_eq!(
        c.send_cmd(&["SET", "s", "1"]).await,
        Frame::SimpleString("OK".to_string())
    );
    assert_eq!(
        c.send_cmd(&["TYPE", "s"]).await,
        Frame::SimpleString("string".to_string())
    );

    assert_eq!(
        c.send_cmd(&["HSET", "h", "f", "v"]).await,
        Frame::Integer(1)
    );
    assert_eq!(
        c.send_cmd(&["TYPE", "h"]).await,
        Frame::SimpleString("hash".to_string())
    );

    assert_eq!(c.send_cmd(&["LPUSH", "l", "x"]).await, Frame::Integer(1));
    assert_eq!(
        c.send_cmd(&["TYPE", "l"]).await,
        Frame::SimpleString("list".to_string())
    );
}
