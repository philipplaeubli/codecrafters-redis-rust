use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fmt::Display,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, SystemTimeError, UNIX_EPOCH},
};

use bytes::Bytes;
use tokio::sync::oneshot;

use crate::parser::RedisType;

pub struct WithExpiry {
    value: Bytes,
    expires: Option<u128>,
}
#[derive(Debug)]
pub enum StoreError {
    KeyNotFound,
    KeyExpired,
    TimeError,
    StreamIdSmallerThanLast,
    StreamIdNotGreaterThan0,
}

impl From<SystemTimeError> for StoreError {
    fn from(_err: SystemTimeError) -> Self {
        StoreError::TimeError
    }
}

enum KeyType {
    Key,
    List,
    Stream,
}

#[derive(Default)]
pub struct Store {
    key_types: HashMap<Bytes, KeyType>,
    streams: HashMap<Bytes, BTreeMap<StreamId, HashMap<Bytes, Bytes>>>,
    keys: HashMap<Bytes, WithExpiry>,
    lists: HashMap<Bytes, Vec<Bytes>>,
    blpop_waiting_queue: HashMap<Bytes, VecDeque<WaitingClient>>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct StreamId {
    pub ms: u128,
    pub seq: u128,
}

/// Represents a client waiting for data
pub struct WaitingClient {
    pub identifier: u64,
    pub sender: oneshot::Sender<RedisType>,
}

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(0);

impl From<StreamId> for RedisType {
    fn from(value: StreamId) -> Self {
        RedisType::BulkString(format!("{}-{}", value.ms, value.seq).into())
    }
}

impl Store {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn rpush(&mut self, key: Bytes, values: Vec<Bytes>) -> Result<usize, StoreError> {
        self.key_types.insert(key.clone(), KeyType::List);
        let list = self.lists.entry(key.clone()).or_default();
        list.extend(values);

        let len = list.len();
        self.notify_first_waiting_client(&key);
        Ok(len)
    }

    pub fn lpush(&mut self, key: Bytes, mut values: Vec<Bytes>) -> Result<usize, StoreError> {
        self.key_types.insert(key.clone(), KeyType::List);
        let list = self.lists.entry(key.clone()).or_default();
        values.reverse(); // reverse the order of the values
        list.splice(0..0, values); //  inserts all the values at the beginning of the list

        let len = list.len();
        self.notify_first_waiting_client(&key);
        Ok(len)
    }

    pub fn get(&self, key: Bytes) -> Result<Bytes, StoreError> {
        let result = self.keys.get(&key).ok_or(StoreError::KeyNotFound)?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();

        if let Some(expiry) = result.expires
            && expiry < now
        {
            return Err(StoreError::KeyExpired);
        }

        Ok(result.value.clone())
    }

    pub fn lrange(
        &self,
        key: Bytes,
        mut start: i128,
        mut end: i128,
    ) -> Result<Vec<Bytes>, StoreError> {
        let list = self.lists.get(&key).ok_or(StoreError::KeyNotFound)?;
        let list_length = list.len() as i128;
        if start < 0 {
            start += list_length;
        }
        if end < 0 {
            end += list_length;
        }

        end += 1;

        if start >= list_length {
            return Ok(vec![]);
        }

        if start < 0 {
            start = 0;
        }

        if end >= list_length {
            end = list_length;
        }

        if start > end {
            return Ok(vec![]);
        }

        let start_pos = start as usize;
        let end_pos = end as usize;

        let slice = &list.as_slice()[start_pos..end_pos];
        Ok(slice.to_vec())
    }

    pub fn set_with_expiry(
        &mut self,
        key: Bytes,
        value: Bytes,
        expiry: Option<u128>,
    ) -> Result<(), StoreError> {
        self.key_types.insert(key.clone(), KeyType::Key);
        let mut expires: Option<u128> = None;
        if let Some(expiry) = expiry {
            let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
            expires = Some(now + expiry);
        }

        let key_value = WithExpiry { value, expires };
        self.keys.insert(key, key_value);
        Ok(())
    }

    pub fn llen(&self, key: &Bytes) -> Result<usize, StoreError> {
        let len = self.lists.get(key).map(|l| l.len()).unwrap_or(0);
        Ok(len)
    }

    pub fn get_type(&self, key: &Bytes) -> Result<Bytes, StoreError> {
        let key_type = self.key_types.get(key);
        if let Some(key_type) = key_type {
            match key_type {
                KeyType::Key => Ok(Bytes::from("string")),
                KeyType::List => Ok(Bytes::from("list")),
                KeyType::Stream => Ok(Bytes::from("stream")),
            }
        } else {
            Ok(Bytes::from("none"))
        }
    }

    pub fn lpop(&mut self, key: Bytes, amount: i128) -> Result<Vec<Bytes>, StoreError> {
        let list = self.lists.entry(key).or_default();

        if !list.is_empty() {
            let removed = list.drain(..amount as usize).collect();
            return Ok(removed);
        }

        Err(StoreError::KeyNotFound)
    }
    /// Pops from list if available, returns the values
    pub fn lpop_for_blpop(&mut self, key: &Bytes) -> Option<Vec<Bytes>> {
        let list = self.lists.get_mut(key)?;
        if list.is_empty() {
            return None;
        }
        let mut removed: Vec<Bytes> = list.drain(..1).collect();
        removed.insert(0, key.clone());
        Some(removed)
    }

    pub fn register_waiting_client(
        &mut self,
        key: Bytes,
        sender: oneshot::Sender<RedisType>,
    ) -> u64 {
        let identifier = NEXT_CLIENT_ID.fetch_add(1, Ordering::SeqCst);
        let client = WaitingClient { identifier, sender };

        self.blpop_waiting_queue
            .entry(key)
            .or_default()
            .push_back(client);

        identifier
    }

    pub fn remove_waiting_client(&mut self, key: &Bytes, client_id: u64) {
        if let Some(queue) = self.blpop_waiting_queue.get_mut(key) {
            queue.retain(|client| client.identifier != client_id);

            // Clean up empty queues
            if queue.is_empty() {
                self.blpop_waiting_queue.remove(key);
            }
        }
    }

    fn notify_first_waiting_client(&mut self, key: &Bytes) {
        let Some(queue) = self.blpop_waiting_queue.get_mut(key) else {
            return;
        };

        let Some(list) = self.lists.get_mut(key) else {
            return;
        };

        if list.is_empty() {
            return;
        }

        if let Some(waiting_client) = queue.pop_front() {
            let value = list.remove(0);
            let response = RedisType::Array(Some(vec![
                RedisType::BulkString(key.clone()),
                RedisType::BulkString(value),
            ]));

            if waiting_client.sender.send(response).is_ok() {
                return;
            }
            // Send failed (client timed out?)
        }

        // Clean up empty queue
        if queue.is_empty() {
            self.blpop_waiting_queue.remove(key);
        }
    }

    pub fn xadd(
        &mut self,
        stream_key: &Bytes,
        seq: Option<u128>,
        ms: Option<u128>,
        args: &[RedisType],
    ) -> Result<StreamId, StoreError> {
        self.key_types.insert(stream_key.clone(), KeyType::Stream);
        let min_stream_id = StreamId { ms: 0, seq: 1 };
        let last_stream_id = self
            .streams
            .get(stream_key) // get the btree
            .and_then(|btree| btree.last_key_value().map(|(id, _)| id.clone()))
            .unwrap_or(StreamId { ms: 0, seq: 0 });

        let stream_id = match (ms, seq) {
            (Some(pot_ms), Some(pot_seq)) => {
                println!(
                    "ms and seq set: Taking stream with ms: {}, seq: {}",
                    pot_ms, pot_seq
                );
                StreamId {
                    ms: pot_ms,
                    seq: pot_seq,
                }
            }
            (Some(pot_ms), None) => {
                if pot_ms == last_stream_id.ms {
                    StreamId {
                        ms: pot_ms,
                        seq: last_stream_id.seq + 1,
                    }
                } else if pot_ms == 0 {
                    StreamId { ms: pot_ms, seq: 1 }
                } else {
                    StreamId { ms: pot_ms, seq: 0 }
                }
            }
            (None, None) => {
                let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
                let new_ms = now.max(last_stream_id.ms);
                if last_stream_id.ms == new_ms {
                    // we already got an entry at that timestamp, increase sequence number
                    StreamId {
                        ms: last_stream_id.ms,
                        seq: last_stream_id.seq + 1,
                    }
                } else if new_ms == 0 {
                    // user input with lowest possible timestamp, we set seq to 1 (redis thing)
                    min_stream_id
                } else {
                    StreamId { ms: new_ms, seq: 0 } // new timestamp, we can start with sequence number 0
                }
            }
            _ => return Err(StoreError::StreamIdSmallerThanLast), // user passed something like *-* or *-1 which is not allowed
        };

        if stream_id < min_stream_id {
            return Err(StoreError::StreamIdNotGreaterThan0);
        }

        match self.streams.entry(stream_key.clone()) {
            std::collections::hash_map::Entry::Occupied(mut existing_entry) => {
                let btree = existing_entry.get_mut();
                match btree.last_key_value() {
                    Some((last_id, _)) => {
                        if last_id >= &stream_id {
                            Err(StoreError::StreamIdSmallerThanLast)
                        } else {
                            Ok(())
                        }
                    }
                    None => Ok(()),
                }?;

                insert_keys_and_values(args, btree.entry(stream_id).or_default());
            }
            std::collections::hash_map::Entry::Vacant(vacant_entry) => {
                let mut btree = BTreeMap::new();
                let mut map = HashMap::new();
                insert_keys_and_values(args, &mut map);
                btree.insert(stream_id, map);
                vacant_entry.insert(btree);
            }
        }

        Ok(stream_id)
    }

    pub fn xrange(
        &self,
        stream_key: &Bytes,
        start_stream_id: StreamId,
        end_stream_id: StreamId,
    ) -> Vec<(StreamId, HashMap<Bytes, Bytes>)> {
        let Some(stream) = self.streams.get(stream_key) else {
            return vec![];
        };
        stream
            .range(start_stream_id..=end_stream_id)
            .map(|(id, entry)| (id.clone(), entry.clone()))
            .collect()
    }
}

fn insert_keys_and_values(arguments: &[RedisType], map: &mut HashMap<Bytes, Bytes>) {
    for chunk in arguments[0..].chunks_exact(2) {
        let field = &chunk[0];
        let value = &chunk[1];
        map.insert(field.to_bytes(), value.to_bytes());
    }
}

#[test]
fn test_lpush() {
    let mut store = Store::new();
    let key = bytes::BytesMut::from("test").freeze();
    let _ = store.lpush(key.clone(), vec!["c".into(), "b".into(), "a".into()]);

    let result = store.lrange(key.clone(), 0, -1).unwrap();
    assert_eq!(
        result,
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );

    let _ = store.lpush(key.clone(), vec!["d".into()]);
    let result = store.lrange(key.clone(), 0, -1).unwrap();
    assert_eq!(
        result,
        vec![
            "d".to_string(),
            "a".to_string(),
            "b".to_string(),
            "c".to_string()
        ]
    );
}

impl Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::KeyNotFound => write!(f, "Key not found"),
            StoreError::KeyExpired => write!(f, "Key expired"),
            StoreError::TimeError => write!(f, "Could not convert time or expiry"),
            StoreError::StreamIdSmallerThanLast => {
                write!(f, "Stream ID smaller than last added Id")
            }
            StoreError::StreamIdNotGreaterThan0 => write!(f, "Stream ID must be greater than 0-0"),
        }
    }
}
