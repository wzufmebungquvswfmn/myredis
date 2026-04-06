use crate::error::{MyRedisError, Result};
use crate::protocol::Frame;
use bytes::Bytes;

/// Parsed command from a RESP frame
#[derive(Debug)]
pub enum Command {
    Ping(Option<Bytes>),
    Set {
        key: Bytes,
        value: Bytes,
    },
    Get {
        key: Bytes,
    },
    Del {
        keys: Vec<Bytes>,
    },
    Exists {
        keys: Vec<Bytes>,
    },
    Incr {
        key: Bytes,
    },
    FlushAll,
    DbSize,
    Type {
        key: Bytes,
    },
    Expire {
        key: Bytes,
        seconds: u64,
    },
    Ttl {
        key: Bytes,
    },
    HSet {
        key: Bytes,
        items: Vec<(Bytes, Bytes)>,
    },
    HGet {
        key: Bytes,
        field: Bytes,
    },
    HDel {
        key: Bytes,
        fields: Vec<Bytes>,
    },
    HLen {
        key: Bytes,
    },
    LPush {
        key: Bytes,
        values: Vec<Bytes>,
    },
    RPush {
        key: Bytes,
        values: Vec<Bytes>,
    },
    LPop {
        key: Bytes,
    },
    RPop {
        key: Bytes,
    },
    LLen {
        key: Bytes,
    },
    Unknown(String),
}

impl Command {
    pub fn from_frame(frame: Frame) -> Result<Command> {
        let args = match frame {
            Frame::Array(a) if !a.is_empty() => a,
            _ => return Err(MyRedisError::Command("expected non-empty array".into())),
        };

        let mut args = args.into_iter();
        let cmd_name = bulk_as_bytes(next_arg(&mut args, "command")?)?;

        if ascii_eq_ignore_case(&cmd_name, b"PING") {
            let message = optional_arg(&mut args, "PING")?
                .map(bulk_as_bytes)
                .transpose()?;
            ensure_no_extra_args(&mut args, "PING")?;
            Ok(Command::Ping(message))
        } else if ascii_eq_ignore_case(&cmd_name, b"GET") {
            let key = bulk_as_bytes(next_arg(&mut args, "GET")?)?;
            ensure_no_extra_args(&mut args, "GET")?;
            Ok(Command::Get { key })
        } else if ascii_eq_ignore_case(&cmd_name, b"SET") {
            let key = bulk_as_bytes(next_arg(&mut args, "SET")?)?;
            let value = bulk_as_bytes(next_arg(&mut args, "SET")?)?;
            ensure_no_extra_args(&mut args, "SET")?;
            Ok(Command::Set { key, value })
        } else if ascii_eq_ignore_case(&cmd_name, b"DEL") {
            let mut keys = Vec::new();
            while let Some(frame) = args.next() {
                keys.push(bulk_as_bytes(frame)?);
            }
            if keys.is_empty() {
                return Err(MyRedisError::Command(
                    "wrong number of arguments for 'DEL'".into(),
                ));
            }
            Ok(Command::Del { keys })
        } else if ascii_eq_ignore_case(&cmd_name, b"EXISTS") {
            let mut keys = Vec::new();
            while let Some(frame) = args.next() {
                keys.push(bulk_as_bytes(frame)?);
            }
            if keys.is_empty() {
                return Err(MyRedisError::Command(
                    "wrong number of arguments for 'EXISTS'".into(),
                ));
            }
            Ok(Command::Exists { keys })
        } else if ascii_eq_ignore_case(&cmd_name, b"INCR") {
            let key = bulk_as_bytes(next_arg(&mut args, "INCR")?)?;
            ensure_no_extra_args(&mut args, "INCR")?;
            Ok(Command::Incr { key })
        } else if ascii_eq_ignore_case(&cmd_name, b"FLUSHALL") {
            ensure_no_extra_args(&mut args, "FLUSHALL")?;
            Ok(Command::FlushAll)
        } else if ascii_eq_ignore_case(&cmd_name, b"DBSIZE") {
            ensure_no_extra_args(&mut args, "DBSIZE")?;
            Ok(Command::DbSize)
        } else if ascii_eq_ignore_case(&cmd_name, b"TYPE") {
            let key = bulk_as_bytes(next_arg(&mut args, "TYPE")?)?;
            ensure_no_extra_args(&mut args, "TYPE")?;
            Ok(Command::Type { key })
        } else if ascii_eq_ignore_case(&cmd_name, b"EXPIRE") {
            let key = bulk_as_bytes(next_arg(&mut args, "EXPIRE")?)?;
            let secs: u64 = bulk_as_utf8(next_arg(&mut args, "EXPIRE")?, "EXPIRE")?
                .parse()
                .map_err(|_| MyRedisError::Command("EXPIRE requires integer seconds".into()))?;
            ensure_no_extra_args(&mut args, "EXPIRE")?;
            Ok(Command::Expire { key, seconds: secs })
        } else if ascii_eq_ignore_case(&cmd_name, b"TTL") {
            let key = bulk_as_bytes(next_arg(&mut args, "TTL")?)?;
            ensure_no_extra_args(&mut args, "TTL")?;
            Ok(Command::Ttl { key })
        } else if ascii_eq_ignore_case(&cmd_name, b"HSET") {
            let key = bulk_as_bytes(next_arg(&mut args, "HSET")?)?;
            let mut items = Vec::new();
            while let Some(field) = args.next() {
                let value = next_arg(&mut args, "HSET")?;
                items.push((bulk_as_bytes(field)?, bulk_as_bytes(value)?));
            }
            if items.is_empty() {
                return Err(MyRedisError::Command(
                    "wrong number of arguments for 'HSET'".into(),
                ));
            }
            Ok(Command::HSet { key, items })
        } else if ascii_eq_ignore_case(&cmd_name, b"HGET") {
            let key = bulk_as_bytes(next_arg(&mut args, "HGET")?)?;
            let field = bulk_as_bytes(next_arg(&mut args, "HGET")?)?;
            ensure_no_extra_args(&mut args, "HGET")?;
            Ok(Command::HGet { key, field })
        } else if ascii_eq_ignore_case(&cmd_name, b"HDEL") {
            let key = bulk_as_bytes(next_arg(&mut args, "HDEL")?)?;
            let mut fields = Vec::new();
            while let Some(frame) = args.next() {
                fields.push(bulk_as_bytes(frame)?);
            }
            if fields.is_empty() {
                return Err(MyRedisError::Command(
                    "wrong number of arguments for 'HDEL'".into(),
                ));
            }
            Ok(Command::HDel { key, fields })
        } else if ascii_eq_ignore_case(&cmd_name, b"HLEN") {
            let key = bulk_as_bytes(next_arg(&mut args, "HLEN")?)?;
            ensure_no_extra_args(&mut args, "HLEN")?;
            Ok(Command::HLen { key })
        } else if ascii_eq_ignore_case(&cmd_name, b"LPUSH") {
            let key = bulk_as_bytes(next_arg(&mut args, "LPUSH")?)?;
            let mut values = Vec::new();
            while let Some(frame) = args.next() {
                values.push(bulk_as_bytes(frame)?);
            }
            if values.is_empty() {
                return Err(MyRedisError::Command(
                    "wrong number of arguments for 'LPUSH'".into(),
                ));
            }
            Ok(Command::LPush { key, values })
        } else if ascii_eq_ignore_case(&cmd_name, b"RPUSH") {
            let key = bulk_as_bytes(next_arg(&mut args, "RPUSH")?)?;
            let mut values = Vec::new();
            while let Some(frame) = args.next() {
                values.push(bulk_as_bytes(frame)?);
            }
            if values.is_empty() {
                return Err(MyRedisError::Command(
                    "wrong number of arguments for 'RPUSH'".into(),
                ));
            }
            Ok(Command::RPush { key, values })
        } else if ascii_eq_ignore_case(&cmd_name, b"LPOP") {
            let key = bulk_as_bytes(next_arg(&mut args, "LPOP")?)?;
            ensure_no_extra_args(&mut args, "LPOP")?;
            Ok(Command::LPop { key })
        } else if ascii_eq_ignore_case(&cmd_name, b"RPOP") {
            let key = bulk_as_bytes(next_arg(&mut args, "RPOP")?)?;
            ensure_no_extra_args(&mut args, "RPOP")?;
            Ok(Command::RPop { key })
        } else if ascii_eq_ignore_case(&cmd_name, b"LLEN") {
            let key = bulk_as_bytes(next_arg(&mut args, "LLEN")?)?;
            ensure_no_extra_args(&mut args, "LLEN")?;
            Ok(Command::LLen { key })
        } else {
            let other = String::from_utf8_lossy(&cmd_name).into_owned();
            Ok(Command::Unknown(other))
        }
    }
}

fn next_arg(iter: &mut impl Iterator<Item = Frame>, cmd: &str) -> Result<Frame> {
    iter.next()
        .ok_or_else(|| MyRedisError::Command(format!("wrong number of arguments for '{}'", cmd)))
}

fn optional_arg(iter: &mut impl Iterator<Item = Frame>, _cmd: &str) -> Result<Option<Frame>> {
    Ok(iter.next())
}

fn ensure_no_extra_args(iter: &mut impl Iterator<Item = Frame>, cmd: &str) -> Result<()> {
    if iter.next().is_some() {
        return Err(MyRedisError::Command(format!(
            "wrong number of arguments for '{}'",
            cmd
        )));
    }
    Ok(())
}

fn bulk_as_bytes(frame: Frame) -> Result<Bytes> {
    match frame {
        Frame::BulkString(b) => Ok(b),
        _ => Err(MyRedisError::Command("expected bulk string".into())),
    }
}

fn bulk_as_utf8(frame: Frame, cmd: &str) -> Result<String> {
    let bytes = bulk_as_bytes(frame)?;
    std::str::from_utf8(&bytes)
        .map(|s| s.to_owned())
        .map_err(|_| MyRedisError::Command(format!("invalid utf8 in argument for '{}'", cmd)))
}

fn ascii_eq_ignore_case(left: &[u8], right: &[u8]) -> bool {
    left.eq_ignore_ascii_case(right)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bulk(input: &str) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(input.as_bytes()))
    }

    #[test]
    fn parses_incr_command() {
        let frame = Frame::Array(vec![bulk("INCR"), bulk("counter")]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::Incr { key } => assert_eq!(key, Bytes::from_static(b"counter")),
            other => panic!("unexpected command: {:?}", other),
        }
    }

    #[test]
    fn rejects_missing_get_argument() {
        let frame = Frame::Array(vec![bulk("GET")]);
        let err = Command::from_frame(frame).unwrap_err();
        assert!(matches!(err, MyRedisError::Command(_)));
    }

    #[test]
    fn ping_with_message_returns_message_payload() {
        let frame = Frame::Array(vec![bulk("PING"), bulk("hello")]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::Ping(Some(message)) => assert_eq!(message, Bytes::from_static(b"hello")),
            other => panic!("unexpected command: {:?}", other),
        }
    }

    #[test]
    fn rejects_extra_arguments() {
        let frame = Frame::Array(vec![bulk("GET"), bulk("foo"), bulk("bar")]);
        let err = Command::from_frame(frame).unwrap_err();
        assert!(matches!(err, MyRedisError::Command(_)));
    }

    #[test]
    fn parses_hset_variadic() {
        let frame = Frame::Array(vec![
            bulk("HSET"),
            bulk("h"),
            bulk("f1"),
            bulk("v1"),
            bulk("f2"),
            bulk("v2"),
        ]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::HSet { key, items } => {
                assert_eq!(key, Bytes::from_static(b"h"));
                assert_eq!(items.len(), 2);
            }
            other => panic!("unexpected command: {:?}", other),
        }
    }

    #[test]
    fn rejects_hset_without_pairs() {
        let frame = Frame::Array(vec![bulk("HSET"), bulk("h")]);
        let err = Command::from_frame(frame).unwrap_err();
        assert!(matches!(err, MyRedisError::Command(_)));
    }

    #[test]
    fn parses_exists_variadic() {
        let frame = Frame::Array(vec![bulk("EXISTS"), bulk("k1"), bulk("k2")]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::Exists { keys } => {
                assert_eq!(keys.len(), 2);
                assert_eq!(keys[0], Bytes::from_static(b"k1"));
                assert_eq!(keys[1], Bytes::from_static(b"k2"));
            }
            other => panic!("unexpected command: {:?}", other),
        }
    }

    #[test]
    fn rejects_exists_without_keys() {
        let frame = Frame::Array(vec![bulk("EXISTS")]);
        let err = Command::from_frame(frame).unwrap_err();
        assert!(matches!(err, MyRedisError::Command(_)));
    }

    #[test]
    fn parses_del_variadic() {
        let frame = Frame::Array(vec![bulk("DEL"), bulk("k1"), bulk("k2")]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::Del { keys } => {
                assert_eq!(keys.len(), 2);
                assert_eq!(keys[0], Bytes::from_static(b"k1"));
                assert_eq!(keys[1], Bytes::from_static(b"k2"));
            }
            other => panic!("unexpected command: {:?}", other),
        }
    }

    #[test]
    fn rejects_del_without_keys() {
        let frame = Frame::Array(vec![bulk("DEL")]);
        let err = Command::from_frame(frame).unwrap_err();
        assert!(matches!(err, MyRedisError::Command(_)));
    }

    #[test]
    fn parses_type_command() {
        let frame = Frame::Array(vec![bulk("TYPE"), bulk("k")]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::Type { key } => assert_eq!(key, Bytes::from_static(b"k")),
            other => panic!("unexpected command: {:?}", other),
        }
    }

    #[test]
    fn unknown_command_with_args_is_unknown_not_wrong_arg_count() {
        let frame = Frame::Array(vec![bulk("NO_SUCH_CMD"), bulk("a"), bulk("b")]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::Unknown(name) => assert_eq!(name, "NO_SUCH_CMD"),
            other => panic!("unexpected command: {:?}", other),
        }
    }
}
