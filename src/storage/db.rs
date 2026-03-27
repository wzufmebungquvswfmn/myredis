use std::sync::Arc;
use std::time::{Duration, Instant};
use std::collections::{HashMap, VecDeque};

use bytes::Bytes;
use dashmap::DashMap;

#[derive(Clone)]
struct Entry {
    value: Value,
    expire_at: Option<Instant>,
}

#[derive(Clone)]
enum Value {
    String(Bytes),
    Hash(HashMap<Bytes, Bytes>),
    List(VecDeque<Bytes>),
}

impl Entry {
    fn is_expired(&self) -> bool {
        self.expire_at.map(|t| Instant::now() > t).unwrap_or(false)
    }
}

/// KV store backed by DashMap.
/// DashMap is internally sharded, so callers still benefit from reduced lock contention.
#[derive(Clone)]
pub struct Db {
    entries: Arc<DashMap<Bytes, Entry>>,
}

impl Db {
    pub fn new(shard_count: usize) -> Self {
        let shard_count = normalize_shard_count(shard_count);
        Db {
            entries: Arc::new(DashMap::with_shard_amount(shard_count)),
        }
    }

    pub fn set(&self, key: Bytes, value: Bytes) {
        self.entries.insert(
            key,
            Entry {
                value: Value::String(value),
                expire_at: None,
            },
        );
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>, &'static str> {
        let entry = match self.entries.get(key) {
            Some(e) => e,
            None => return Ok(None),
        };
        if entry.is_expired() {
            drop(entry);
            self.entries.remove(key);
            return Ok(None);
        }
        match &entry.value {
            Value::String(v) => Ok(Some(v.clone())),
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        }
    }

    pub fn del(&self, key: &[u8]) -> bool {
        self.entries.remove(key).is_some()
    }

    pub fn exists(&self, keys: &[Bytes]) -> i64 {
        let mut count = 0i64;
        for key in keys {
            match self.entries.get(key.as_ref()) {
                None => {}
                Some(entry) if entry.is_expired() => {
                    drop(entry);
                    self.entries.remove(key.as_ref());
                }
                Some(_) => count += 1,
            }
        }
        count
    }

    pub fn incr(&self, key: &[u8]) -> Result<i64, &'static str> {
        let mut entry = self.entries.entry(Bytes::copy_from_slice(key)).or_insert(Entry {
            value: Value::String(Bytes::from_static(b"0")),
            expire_at: None,
        });

        if entry.is_expired() {
            entry.value = Value::String(Bytes::from_static(b"0"));
            entry.expire_at = None;
        }

        let current_value = match &entry.value {
            Value::String(v) => v,
            _ => {
                return Err("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
        };

        let current = std::str::from_utf8(current_value.as_ref())
            .map_err(|_| "value is not valid utf8")?
            .parse::<i64>()
            .map_err(|_| "value is not an integer")?;

        let next = current.checked_add(1).ok_or("increment would overflow")?;
        entry.value = Value::String(Bytes::from(next.to_string()));
        Ok(next)
    }

    pub fn hset(&self, key: Bytes, items: Vec<(Bytes, Bytes)>) -> Result<i64, &'static str> {
        let mut entry = self.entries.entry(key).or_insert(Entry {
            value: Value::Hash(HashMap::new()),
            expire_at: None,
        });

        if entry.is_expired() {
            entry.value = Value::Hash(HashMap::new());
            entry.expire_at = None;
        }

        let hash = match &mut entry.value {
            Value::Hash(h) => h,
            _ => {
                return Err("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
        };

        let mut created = 0i64;
        for (field, value) in items {
            if hash.insert(field, value).is_none() {
                created += 1;
            }
        }
        Ok(created)
    }

    pub fn hget(&self, key: &[u8], field: &[u8]) -> Result<Option<Bytes>, &'static str> {
        let entry = match self.entries.get(key) {
            Some(e) => e,
            None => return Ok(None),
        };
        if entry.is_expired() {
            drop(entry);
            self.entries.remove(key);
            return Ok(None);
        }
        match &entry.value {
            Value::Hash(h) => Ok(h.get(field).cloned()),
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        }
    }

    pub fn hdel(&self, key: &[u8], fields: &[Bytes]) -> Result<i64, &'static str> {
        let mut entry = match self.entries.get_mut(key) {
            Some(e) => e,
            None => return Ok(0),
        };
        if entry.is_expired() {
            drop(entry);
            self.entries.remove(key);
            return Ok(0);
        }

        let hash = match &mut entry.value {
            Value::Hash(h) => h,
            _ => {
                return Err("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
        };

        let mut removed = 0i64;
        for field in fields {
            if hash.remove(field.as_ref()).is_some() {
                removed += 1;
            }
        }
        Ok(removed)
    }

    pub fn hlen(&self, key: &[u8]) -> Result<i64, &'static str> {
        let entry = match self.entries.get(key) {
            Some(e) => e,
            None => return Ok(0),
        };
        if entry.is_expired() {
            drop(entry);
            self.entries.remove(key);
            return Ok(0);
        }
        match &entry.value {
            Value::Hash(h) => Ok(h.len() as i64),
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        }
    }

    pub fn lpush(&self, key: Bytes, values: Vec<Bytes>) -> Result<i64, &'static str> {
        self.push_list(key, values, true)
    }

    pub fn rpush(&self, key: Bytes, values: Vec<Bytes>) -> Result<i64, &'static str> {
        self.push_list(key, values, false)
    }

    fn push_list(&self, key: Bytes, values: Vec<Bytes>, to_left: bool) -> Result<i64, &'static str> {
        let mut entry = self.entries.entry(key).or_insert(Entry {
            value: Value::List(VecDeque::new()),
            expire_at: None,
        });

        if entry.is_expired() {
            entry.value = Value::List(VecDeque::new());
            entry.expire_at = None;
        }

        let list = match &mut entry.value {
            Value::List(l) => l,
            _ => {
                return Err("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
        };

        for v in values {
            if to_left {
                list.push_front(v);
            } else {
                list.push_back(v);
            }
        }
        Ok(list.len() as i64)
    }

    pub fn lpop(&self, key: &[u8]) -> Result<Option<Bytes>, &'static str> {
        self.pop_list(key, true)
    }

    pub fn rpop(&self, key: &[u8]) -> Result<Option<Bytes>, &'static str> {
        self.pop_list(key, false)
    }

    fn pop_list(&self, key: &[u8], from_left: bool) -> Result<Option<Bytes>, &'static str> {
        let mut entry = match self.entries.get_mut(key) {
            Some(e) => e,
            None => return Ok(None),
        };
        if entry.is_expired() {
            drop(entry);
            self.entries.remove(key);
            return Ok(None);
        }

        let list = match &mut entry.value {
            Value::List(l) => l,
            _ => {
                return Err("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
        };

        let value = if from_left {
            list.pop_front()
        } else {
            list.pop_back()
        };

        let empty = list.is_empty();
        drop(entry);
        if empty {
            self.entries.remove(key);
        }

        Ok(value)
    }

    pub fn llen(&self, key: &[u8]) -> Result<i64, &'static str> {
        let entry = match self.entries.get(key) {
            Some(e) => e,
            None => return Ok(0),
        };
        if entry.is_expired() {
            drop(entry);
            self.entries.remove(key);
            return Ok(0);
        }
        match &entry.value {
            Value::List(l) => Ok(l.len() as i64),
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
        }
    }

    pub fn expire(&self, key: &[u8], seconds: u64) -> bool {
        if let Some(mut entry) = self.entries.get_mut(key) {
            entry.expire_at = Some(Instant::now() + Duration::from_secs(seconds));
            true
        } else {
            false
        }
    }

    /// Returns remaining TTL in seconds, -1 if no expiry, -2 if key not found
    pub fn ttl(&self, key: &[u8]) -> i64 {
        match self.entries.get(key) {
            None => -2,
            Some(entry) if entry.is_expired() => {
                drop(entry);
                self.entries.remove(key);
                -2
            }
            Some(entry) => match entry.expire_at {
                None => -1,
                Some(t) => {
                    let now = Instant::now();
                    if t > now {
                        (t - now).as_secs() as i64
                    } else {
                        -2
                    }
                }
            },
        }
    }
}

fn normalize_shard_count(shard_count: usize) -> usize {
    shard_count.max(2).next_power_of_two()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn test_set_get_del() {
        let db = Db::new(16);
        db.set(Bytes::from_static(b"foo"), Bytes::from_static(b"bar"));
        assert_eq!(db.get(b"foo"), Ok(Some(Bytes::from_static(b"bar"))));
        assert!(db.del(b"foo"));
        assert_eq!(db.get(b"foo"), Ok(None));
    }

    #[test]
    fn test_expire_ttl() {
        let db = Db::new(16);
        db.set(Bytes::from_static(b"k"), Bytes::from_static(b"v"));
        db.expire(b"k", 1);
        assert!(db.ttl(b"k") >= 0);
        sleep(Duration::from_millis(1100));
        assert_eq!(db.get(b"k"), Ok(None));
        assert_eq!(db.ttl(b"k"), -2);
    }

    #[test]
    fn test_exists_counts_and_cleans_expired() {
        let db = Db::new(16);
        db.set(Bytes::from_static(b"a"), Bytes::from_static(b"1"));
        db.set(Bytes::from_static(b"b"), Bytes::from_static(b"2"));
        db.expire(b"b", 1);

        assert_eq!(
            db.exists(&[
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
                Bytes::from_static(b"c"),
            ]),
            2
        );

        sleep(Duration::from_millis(1100));
        assert_eq!(
            db.exists(&[
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
                Bytes::from_static(b"c"),
            ]),
            1
        );
    }

    #[test]
    fn test_incr_creates_and_updates_counter() {
        let db = Db::new(16);
        assert_eq!(db.incr(b"counter"), Ok(1));
        assert_eq!(db.incr(b"counter"), Ok(2));
        assert_eq!(db.get(b"counter"), Ok(Some(Bytes::from_static(b"2"))));
    }

    #[test]
    fn test_incr_rejects_non_integer_value() {
        let db = Db::new(16);
        db.set(Bytes::from_static(b"counter"), Bytes::from_static(b"abc"));
        assert_eq!(db.incr(b"counter"), Err("value is not an integer"));
    }

    #[test]
    fn test_clone_shares_state() {
        let db = Db::new(16);
        let clone = db.clone();
        db.set(Bytes::from_static(b"shared"), Bytes::from_static(b"value"));
        assert_eq!(clone.get(b"shared"), Ok(Some(Bytes::from_static(b"value"))));
    }

    #[test]
    fn test_hash_commands() {
        let db = Db::new(16);
        assert_eq!(
            db.hset(
                Bytes::from_static(b"h"),
                vec![(Bytes::from_static(b"f1"), Bytes::from_static(b"v1"))]
            ),
            Ok(1)
        );
        assert_eq!(db.hget(b"h", b"f1"), Ok(Some(Bytes::from_static(b"v1"))));
        assert_eq!(db.hlen(b"h"), Ok(1));
        assert_eq!(db.hdel(b"h", &[Bytes::from_static(b"f1")]), Ok(1));
        assert_eq!(db.hlen(b"h"), Ok(0));
    }

    #[test]
    fn test_list_commands() {
        let db = Db::new(16);
        assert_eq!(
            db.rpush(
                Bytes::from_static(b"l"),
                vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]
            ),
            Ok(2)
        );
        assert_eq!(db.lpush(Bytes::from_static(b"l"), vec![Bytes::from_static(b"x")]), Ok(3));
        assert_eq!(db.llen(b"l"), Ok(3));
        assert_eq!(db.lpop(b"l"), Ok(Some(Bytes::from_static(b"x"))));
        assert_eq!(db.rpop(b"l"), Ok(Some(Bytes::from_static(b"b"))));
        assert_eq!(db.rpop(b"l"), Ok(Some(Bytes::from_static(b"a"))));
        assert_eq!(db.rpop(b"l"), Ok(None));
    }
}
