use bytes::Bytes;

/// RESP protocol frame types
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Bytes),
    Null,
    Array(Vec<Frame>),
}

impl Frame {
    /// Encode frame to RESP bytes
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(64);
        self.encode_into(&mut out);
        out
    }

    pub fn encode_into(&self, out: &mut Vec<u8>) {
        self.write_to(out);
    }

    fn write_to(&self, out: &mut Vec<u8>) {
        match self {
            Frame::SimpleString(s) => {
                out.push(b'+');
                out.extend_from_slice(s.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Frame::Error(e) => {
                out.push(b'-');
                out.extend_from_slice(e.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Frame::Integer(n) => {
                out.push(b':');
                out.extend_from_slice(n.to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Frame::BulkString(b) => {
                out.push(b'$');
                out.extend_from_slice(b.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(b);
                out.extend_from_slice(b"\r\n");
            }
            Frame::Null => out.extend_from_slice(b"$-1\r\n"),
            Frame::Array(items) => {
                out.push(b'*');
                out.extend_from_slice(items.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                for item in items {
                    item.write_to(out);
                }
            }
        }
    }
}
