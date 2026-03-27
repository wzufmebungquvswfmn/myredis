use super::frame::Frame;
use crate::error::{MyRedisError, Result};
use bytes::Bytes;
use bytes::BytesMut;

/// Try to parse one RESP frame from the buffer.
/// Returns Ok(Some(frame)) if a complete frame was parsed.
/// Returns Ok(None) if data is incomplete (need more bytes).
/// Returns Err if the data is malformed.
pub fn parse_frame(buf: &mut BytesMut) -> Result<Option<Frame>> {
    if buf.is_empty() {
        return Ok(None);
    }

    match try_parse(buf) {
        Ok((frame, consumed)) => {
            let _ = buf.split_to(consumed);
            Ok(Some(frame))
        }
        Err(MyRedisError::Incomplete) => Ok(None),
        Err(e) => Err(e),
    }
}

fn try_parse(buf: &[u8]) -> Result<(Frame, usize)> {
    if buf.is_empty() {
        return Err(MyRedisError::Incomplete);
    }

    match buf[0] {
        b'+' => parse_simple_string(buf),
        b'-' => parse_error(buf),
        b':' => parse_integer(buf),
        b'$' => parse_bulk_string(buf),
        b'*' => parse_array(buf),
        b => Err(MyRedisError::Protocol(format!(
            "unknown type byte: {}",
            b as char
        ))),
    }
}

fn read_line(buf: &[u8]) -> Result<(&[u8], usize)> {
    for i in 0..buf.len().saturating_sub(1) {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Ok((&buf[..i], i + 2));
        }
    }
    Err(MyRedisError::Incomplete)
}

fn parse_simple_string(buf: &[u8]) -> Result<(Frame, usize)> {
    let (line, consumed) = read_line(&buf[1..])?;
    let s = std::str::from_utf8(line)
        .map_err(|_| MyRedisError::Protocol("invalid utf8 in simple string".into()))?;
    Ok((Frame::SimpleString(s.to_string()), consumed + 1))
}

fn parse_error(buf: &[u8]) -> Result<(Frame, usize)> {
    let (line, consumed) = read_line(&buf[1..])?;
    let s = std::str::from_utf8(line)
        .map_err(|_| MyRedisError::Protocol("invalid utf8 in error".into()))?;
    Ok((Frame::Error(s.to_string()), consumed + 1))
}

fn parse_integer(buf: &[u8]) -> Result<(Frame, usize)> {
    let (line, consumed) = read_line(&buf[1..])?;
    let s = std::str::from_utf8(line)
        .map_err(|_| MyRedisError::Protocol("invalid utf8 in integer".into()))?;
    let n: i64 = s
        .parse()
        .map_err(|_| MyRedisError::Protocol(format!("invalid integer: {}", s)))?;
    Ok((Frame::Integer(n), consumed + 1))
}

fn parse_bulk_string(buf: &[u8]) -> Result<(Frame, usize)> {
    let (line, header_consumed) = read_line(&buf[1..])?;
    let s = std::str::from_utf8(line)
        .map_err(|_| MyRedisError::Protocol("invalid bulk string length".into()))?;
    let len: i64 = s
        .parse()
        .map_err(|_| MyRedisError::Protocol(format!("invalid bulk string length: {}", s)))?;

    if len < -1 {
        return Err(MyRedisError::Protocol(format!(
            "invalid bulk string length: {}",
            s
        )));
    }

    if len == -1 {
        return Ok((Frame::Null, header_consumed + 1));
    }

    let len = len as usize;
    let start = header_consumed + 1;
    let end = start + len;

    if buf.len() < end + 2 {
        return Err(MyRedisError::Incomplete);
    }

    if &buf[end..end + 2] != b"\r\n" {
        return Err(MyRedisError::Protocol(
            "bulk string missing CRLF terminator".into(),
        ));
    }

    let data = Bytes::copy_from_slice(&buf[start..end]);
    Ok((Frame::BulkString(data), end + 2))
}

fn parse_array(buf: &[u8]) -> Result<(Frame, usize)> {
    let (line, header_consumed) = read_line(&buf[1..])?;
    let s = std::str::from_utf8(line)
        .map_err(|_| MyRedisError::Protocol("invalid array length".into()))?;
    let count: i64 = s
        .parse()
        .map_err(|_| MyRedisError::Protocol(format!("invalid array length: {}", s)))?;

    if count < -1 {
        return Err(MyRedisError::Protocol(format!(
            "invalid array length: {}",
            s
        )));
    }

    if count == -1 {
        return Ok((Frame::Null, header_consumed + 1));
    }

    let count = count as usize;
    let mut items = Vec::with_capacity(count);
    let mut offset = header_consumed + 1;

    for _ in 0..count {
        let (frame, consumed) = try_parse(&buf[offset..])?;
        items.push(frame);
        offset += consumed;
    }

    Ok((Frame::Array(items), offset))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_parse_ping() {
        let mut buf = BytesMut::from(&b"*1\r\n$4\r\nPING\r\n"[..]);
        let frame = parse_frame(&mut buf).unwrap().unwrap();
        assert_eq!(
            frame,
            Frame::Array(vec![Frame::BulkString(Bytes::from_static(b"PING"))])
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn test_parse_set() {
        let mut buf = BytesMut::from(&b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"[..]);
        let frame = parse_frame(&mut buf).unwrap().unwrap();
        assert_eq!(
            frame,
            Frame::Array(vec![
                Frame::BulkString(Bytes::from_static(b"SET")),
                Frame::BulkString(Bytes::from_static(b"foo")),
                Frame::BulkString(Bytes::from_static(b"bar")),
            ])
        );
    }

    #[test]
    fn test_incomplete() {
        let mut buf = BytesMut::from(&b"*1\r\n$4\r\nPI"[..]);
        let result = parse_frame(&mut buf).unwrap();
        assert!(result.is_none());
        // buffer should be unchanged
        assert_eq!(buf.len(), 10);
    }

    #[test]
    fn test_two_commands_in_buffer() {
        let mut buf = BytesMut::from(&b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n"[..]);
        let f1 = parse_frame(&mut buf).unwrap().unwrap();
        let f2 = parse_frame(&mut buf).unwrap().unwrap();
        assert_eq!(f1, f2);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_invalid_bulk_string_terminator() {
        let mut buf = BytesMut::from(&b"$3\r\nbarxx"[..]);
        let err = parse_frame(&mut buf).unwrap_err();
        assert!(matches!(err, MyRedisError::Protocol(_)));
    }

    #[test]
    fn test_nested_array_incomplete() {
        let mut buf = BytesMut::from(&b"*2\r\n*1\r\n$4\r\nPING\r\n$3\r\nba"[..]);
        let result = parse_frame(&mut buf).unwrap();
        assert!(result.is_none());
    }
}
