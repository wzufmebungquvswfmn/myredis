/// Simple benchmark client for myredis.
/// Usage: cargo run --bin bench -- [--host 127.0.0.1] [--port 6379]
///                                  [--clients 50] [--requests 10000]
///                                  [--mode set|get|mixed]
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Clone)]
struct Config {
    host: String,
    port: u16,
    clients: usize,
    requests: usize,
    mode: String,
    pipeline: usize,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            host: "127.0.0.1".into(),
            port: 6379,
            clients: 50,
            requests: 10000,
            mode: "mixed".into(),
            pipeline: 1,
        }
    }
}

fn parse_args() -> Config {
    let args: Vec<String> = std::env::args().collect();
    let mut cfg = Config::default();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--host" => { cfg.host = args[i + 1].clone(); i += 2; }
            "--port" => { cfg.port = args[i + 1].parse().unwrap(); i += 2; }
            "--clients" => { cfg.clients = args[i + 1].parse().unwrap(); i += 2; }
            "--requests" => { cfg.requests = args[i + 1].parse().unwrap(); i += 2; }
            "--mode" => { cfg.mode = args[i + 1].clone(); i += 2; }
            "--pipeline" => { cfg.pipeline = args[i + 1].parse().unwrap(); i += 2; }
            _ => { i += 1; }
        }
    }
    cfg.pipeline = cfg.pipeline.max(1);
    cfg
}

fn build_set(key: &str, value: &str) -> Vec<u8> {
    format!("*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
        key.len(), key, value.len(), value).into_bytes()
}

fn build_get(key: &str) -> Vec<u8> {
    format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key).into_bytes()
}

async fn run_client(cfg: Config, requests: usize, latencies: Arc<Vec<AtomicU64>>, client_id: usize) {
    let addr = format!("{}:{}", cfg.host, cfg.port);
    let mut stream = TcpStream::connect(&addr).await.expect("connect failed");
    stream.set_nodelay(true).expect("set TCP_NODELAY failed");
    let mut read_buf = vec![0u8; 4096];
    let mut response_buf = BytesMut::with_capacity(4096);

    let pipeline = cfg.pipeline.min(requests);
    let mut i = 0;
    while i < requests {
        let batch = pipeline.min(requests - i);
        let mut batch_buf = Vec::with_capacity(batch * 64);

        for offset in 0..batch {
            let request_index = i + offset;
            let key = format!("key:{}", (client_id * requests + request_index) % 1000);
            let value = "hello_myredis";

            let cmd = match cfg.mode.as_str() {
                "set" => build_set(&key, value),
                "get" => build_get(&key),
                _ => if request_index % 2 == 0 { build_set(&key, value) } else { build_get(&key) },
            };
            batch_buf.extend_from_slice(&cmd);
        }

        let start = Instant::now();
        stream.write_all(&batch_buf).await.unwrap();

        for _ in 0..batch {
            read_one_response(&mut stream, &mut read_buf, &mut response_buf).await
                .expect("read response failed");
        }

        let elapsed_us = start.elapsed().as_micros() as u64;
        let per_request_us = elapsed_us / batch as u64;

        for offset in 0..batch {
            let idx = client_id * requests + i + offset;
            if idx < latencies.len() {
                latencies[idx].store(per_request_us, Ordering::Relaxed);
            }
        }

        i += batch;
    }
}

async fn read_one_response(
    stream: &mut TcpStream,
    read_buf: &mut [u8],
    response_buf: &mut BytesMut,
) -> std::io::Result<()> {
    loop {
        if let Some(consumed) = response_len(response_buf) {
            let _ = response_buf.split_to(consumed);
            return Ok(());
        }

        let n = stream.read(read_buf).await?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "server closed connection",
            ));
        }
        response_buf.extend_from_slice(&read_buf[..n]);
    }
}

fn response_len(buf: &[u8]) -> Option<usize> {
    let first = *buf.first()?;
    match first {
        b'+' | b'-' | b':' => find_crlf(buf).map(|idx| idx + 2),
        b'$' => {
            let line_end = find_crlf(buf)?;
            let len = std::str::from_utf8(&buf[1..line_end]).ok()?.parse::<isize>().ok()?;
            if len == -1 {
                return Some(line_end + 2);
            }
            if len < -1 {
                return None;
            }
            let total = line_end + 2 + len as usize + 2;
            (buf.len() >= total).then_some(total)
        }
        _ => None,
    }
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}

#[tokio::main]
async fn main() {
    let cfg = parse_args();
    let total = cfg.clients * cfg.requests;

    println!("myredis bench");
    println!("  host:     {}:{}", cfg.host, cfg.port);
    println!("  clients:  {}", cfg.clients);
    println!("  requests: {} per client ({} total)", cfg.requests, total);
    println!("  mode:     {}", cfg.mode);
    println!("  pipeline: {}", cfg.pipeline);
    println!();

    let latencies: Arc<Vec<AtomicU64>> = Arc::new(
        (0..total).map(|_| AtomicU64::new(0)).collect()
    );

    let start = Instant::now();
    let mut handles = Vec::new();

    for c in 0..cfg.clients {
        let cfg2 = cfg.clone();
        let lat = latencies.clone();
        handles.push(tokio::spawn(async move {
            run_client(cfg2, cfg.requests, lat, c).await;
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let elapsed = start.elapsed();
    let qps = total as f64 / elapsed.as_secs_f64();

    // collect latencies
    let mut lats: Vec<u64> = latencies.iter()
        .map(|a| a.load(Ordering::Relaxed))
        .filter(|&v| v > 0)
        .collect();
    lats.sort_unstable();

    let avg = lats.iter().sum::<u64>() as f64 / lats.len() as f64;
    let p50 = lats[lats.len() * 50 / 100];
    let p95 = lats[lats.len() * 95 / 100];
    let p99 = lats[lats.len() * 99 / 100];

    println!("Results:");
    println!("  total time:  {:.2}s", elapsed.as_secs_f64());
    println!("  QPS:         {:.0}", qps);
    println!("  avg latency: {:.1} us", avg);
    println!("  P50:         {} us", p50);
    println!("  P95:         {} us", p95);
    println!("  P99:         {} us", p99);
}
