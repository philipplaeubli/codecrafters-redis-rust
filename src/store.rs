use std::{
    collections::{HashMap, VecDeque},
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
}

impl From<SystemTimeError> for StoreError {
    fn from(_err: SystemTimeError) -> Self {
        StoreError::TimeError
    }
}
pub struct Store {
    keys: HashMap<Bytes, WithExpiry>,
    lists: HashMap<Bytes, Vec<Bytes>>,
    blpop_queue: HashMap<Bytes, VecDeque<WaitingClient>>,
}
/// Represents a client waiting for data
pub struct WaitingClient {
    pub identifier: u64,
    pub sender: oneshot::Sender<RedisType>,
}

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(0);

impl Store {
    pub fn new() -> Self {
        Store {
            keys: HashMap::new(),
            lists: HashMap::new(),
            blpop_queue: HashMap::new(),
        }
    }

    pub fn rpush(&mut self, key: Bytes, values: Vec<Bytes>) -> Result<usize, StoreError> {
        let list = self.lists.entry(key.clone()).or_default();
        list.append(&mut values.clone());
        let len = list.len();
        self.notify_blocked_clients(&key);

        Ok(len)
    }

    pub fn lpush(&mut self, key: Bytes, mut values: Vec<Bytes>) -> Result<usize, StoreError> {
        let list = self.lists.entry(key.clone()).or_default();
        values.reverse(); // reverse the order of the values
        list.splice(0..0, values); //  inserts all the values at the beginning of the list
        let len = list.len();
        self.notify_blocked_clients(&key);

        Ok(len)
    }

    pub fn get(&self, key: Bytes) -> Result<Bytes, StoreError> {
        let result = self.keys.get(&key).ok_or(StoreError::KeyNotFound)?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();

        if let Some(expiry) = result.expires {
            if expiry < now {
                return Err(StoreError::KeyExpired);
            }
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
            start = list_length + start;
        }
        if end < 0 {
            end = list_length + end;
        }

        end = end + 1;

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
        let mut expires: Option<u128> = None;
        if let Some(expiry) = expiry {
            let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
            expires = Some(now + expiry);
        }

        let key_value = WithExpiry { value, expires };
        self.keys.insert(key, key_value);
        Ok(())
    }

    pub fn llen(&mut self, key: Bytes) -> Result<usize, StoreError> {
        let list = self.lists.entry(key).or_default();
        Ok(list.len())
    }

    pub fn lpop(&mut self, key: Bytes, amount: i128) -> Result<Vec<Bytes>, StoreError> {
        let list = self.lists.entry(key).or_default();

        if list.len() > 0 {
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

    pub fn register_blocked_client(
        &mut self,
        key: Bytes,
        sender: oneshot::Sender<RedisType>,
    ) -> u64 {
        let identifier = NEXT_CLIENT_ID.fetch_add(1, Ordering::SeqCst);
        let client = WaitingClient { identifier, sender };

        self.blpop_queue.entry(key).or_default().push_back(client);

        identifier
    }

    pub fn remove_blocked_client(&mut self, key: &Bytes, client_id: u64) {
        if let Some(queue) = self.blpop_queue.get_mut(key) {
            queue.retain(|client| client.identifier != client_id);

            // Clean up empty queues
            if queue.is_empty() {
                self.blpop_queue.remove(key);
            }
        }
    }

    fn notify_blocked_clients(&mut self, key: &Bytes) {
        let Some(queue) = self.blpop_queue.get_mut(key) else {
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
            self.blpop_queue.remove(key);
        }
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
