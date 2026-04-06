use bytes::BytesMut;
use std::io::ErrorKind;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, error, warn};

use crate::command::Command;
use crate::protocol::{parse_frame, Frame};
use crate::storage::Db;

pub struct Connection {
    stream: TcpStream,
    buf: BytesMut,
    write_buf: Vec<u8>,
    db: Db,
}

impl Connection {
    pub fn new(stream: TcpStream, db: Db) -> Self {
        Connection {
            stream,
            buf: BytesMut::with_capacity(4096),
            write_buf: Vec::with_capacity(256),
            db,
        }
    }

    /// Main loop: read -> parse -> execute -> respond
    pub async fn run(mut self) {
        loop {
            // Read more data into buffer
            match self.stream.read_buf(&mut self.buf).await {
                Ok(0) => {
                    debug!("client disconnected");
                    return;
                }
                Ok(n) => debug!("read {} bytes", n),
                Err(e) => {
                    if matches!(
                        e.kind(),
                        ErrorKind::ConnectionReset
                            | ErrorKind::ConnectionAborted
                            | ErrorKind::UnexpectedEof
                            | ErrorKind::BrokenPipe
                    ) {
                        debug!("client disconnected abruptly: {}", e);
                    } else {
                        error!("read error: {}", e);
                    }
                    return;
                }
            }

            // Process all complete frames in buffer
            loop {
                match parse_frame(&mut self.buf) {
                    Ok(Some(frame)) => {
                        self.handle_frame(frame);
                    }
                    Ok(None) => break, // need more data
                    Err(e) => {
                        warn!("parse error: {}", e);
                        self.write_buf.clear();
                        let err_frame = Frame::Error(format!("ERR {}", e));
                        err_frame.encode_into(&mut self.write_buf);
                        let _ = self.stream.write_all(&self.write_buf).await;
                        return;
                    }
                }
            }

            if !self.write_buf.is_empty() {
                if let Err(e) = self.stream.write_all(&self.write_buf).await {
                    error!("write error: {}", e);
                    return;
                }
                self.write_buf.clear();
            }
        }
    }

    fn handle_frame(&mut self, frame: Frame) {
        let cmd = match Command::from_frame(frame) {
            Ok(c) => c,
            Err(e) => {
                Frame::Error(format!("ERR {}", e)).encode_into(&mut self.write_buf);
                return;
            }
        };

        match cmd {
            Command::Ping(None) => self.write_buf.extend_from_slice(b"+PONG\r\n"),
            Command::Ping(Some(message)) => {
                Frame::BulkString(message).encode_into(&mut self.write_buf)
            }
            Command::Set { key, value } => {
                self.db.set(key, value);
                self.write_buf.extend_from_slice(b"+OK\r\n");
            }
            Command::Get { key } => match self.db.get(&key) {
                Ok(Some(v)) => Frame::BulkString(v).encode_into(&mut self.write_buf),
                Ok(None) => self.write_buf.extend_from_slice(b"$-1\r\n"),
                Err(message) => {
                    Frame::Error(format!("ERR {}", message)).encode_into(&mut self.write_buf)
                }
            },
            Command::Del { keys } => {
                let deleted = keys.iter().map(|key| self.db.del(key) as i64).sum::<i64>();
                Frame::Integer(deleted).encode_into(&mut self.write_buf);
            }
            Command::Exists { keys } => {
                Frame::Integer(self.db.exists(&keys)).encode_into(&mut self.write_buf);
            }
            Command::Incr { key } => match self.db.incr(&key) {
                Ok(value) => Frame::Integer(value).encode_into(&mut self.write_buf),
                Err(message) => {
                    Frame::Error(format!("ERR {}", message)).encode_into(&mut self.write_buf)
                }
            },
            Command::FlushAll => {
                self.db.flush_all();
                self.write_buf.extend_from_slice(b"+OK\r\n");
            }
            Command::DbSize => {
                Frame::Integer(self.db.dbsize()).encode_into(&mut self.write_buf);
            }
            Command::Type { key } => {
                Frame::SimpleString(self.db.key_type(&key).to_string())
                    .encode_into(&mut self.write_buf);
            }
            Command::Expire { key, seconds } => {
                let ok = self.db.expire(&key, seconds) as i64;
                Frame::Integer(ok).encode_into(&mut self.write_buf);
            }
            Command::Ttl { key } => {
                Frame::Integer(self.db.ttl(&key)).encode_into(&mut self.write_buf)
            }
            Command::HSet { key, items } => match self.db.hset(key, items) {
                Ok(created) => Frame::Integer(created).encode_into(&mut self.write_buf),
                Err(message) => {
                    Frame::Error(format!("ERR {}", message)).encode_into(&mut self.write_buf)
                }
            },
            Command::HGet { key, field } => match self.db.hget(&key, &field) {
                Ok(Some(v)) => Frame::BulkString(v).encode_into(&mut self.write_buf),
                Ok(None) => self.write_buf.extend_from_slice(b"$-1\r\n"),
                Err(message) => {
                    Frame::Error(format!("ERR {}", message)).encode_into(&mut self.write_buf)
                }
            },
            Command::HDel { key, fields } => match self.db.hdel(&key, &fields) {
                Ok(removed) => Frame::Integer(removed).encode_into(&mut self.write_buf),
                Err(message) => {
                    Frame::Error(format!("ERR {}", message)).encode_into(&mut self.write_buf)
                }
            },
            Command::HLen { key } => match self.db.hlen(&key) {
                Ok(len) => Frame::Integer(len).encode_into(&mut self.write_buf),
                Err(message) => {
                    Frame::Error(format!("ERR {}", message)).encode_into(&mut self.write_buf)
                }
            },
            Command::LPush { key, values } => match self.db.lpush(key, values) {
                Ok(len) => Frame::Integer(len).encode_into(&mut self.write_buf),
                Err(message) => {
                    Frame::Error(format!("ERR {}", message)).encode_into(&mut self.write_buf)
                }
            },
            Command::RPush { key, values } => match self.db.rpush(key, values) {
                Ok(len) => Frame::Integer(len).encode_into(&mut self.write_buf),
                Err(message) => {
                    Frame::Error(format!("ERR {}", message)).encode_into(&mut self.write_buf)
                }
            },
            Command::LPop { key } => match self.db.lpop(&key) {
                Ok(Some(v)) => Frame::BulkString(v).encode_into(&mut self.write_buf),
                Ok(None) => self.write_buf.extend_from_slice(b"$-1\r\n"),
                Err(message) => {
                    Frame::Error(format!("ERR {}", message)).encode_into(&mut self.write_buf)
                }
            },
            Command::RPop { key } => match self.db.rpop(&key) {
                Ok(Some(v)) => Frame::BulkString(v).encode_into(&mut self.write_buf),
                Ok(None) => self.write_buf.extend_from_slice(b"$-1\r\n"),
                Err(message) => {
                    Frame::Error(format!("ERR {}", message)).encode_into(&mut self.write_buf)
                }
            },
            Command::LLen { key } => match self.db.llen(&key) {
                Ok(len) => Frame::Integer(len).encode_into(&mut self.write_buf),
                Err(message) => {
                    Frame::Error(format!("ERR {}", message)).encode_into(&mut self.write_buf)
                }
            },
            Command::Unknown(name) => {
                Frame::Error(format!("ERR unknown command '{}'", name))
                    .encode_into(&mut self.write_buf);
            }
        }
    }
}
