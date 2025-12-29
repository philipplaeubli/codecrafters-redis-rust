use std::{
    collections::HashMap,
    ops::Deref,
    sync::Arc,
    time::{SystemTime, SystemTimeError, UNIX_EPOCH},
};

use bytes::{Bytes, BytesMut};
use tokio::sync::RwLock;

pub struct WithExpiry {
    value: Bytes,
    expires: Option<u128>,
}

pub struct Store {
    keys: HashMap<Bytes, WithExpiry>,
    lists: HashMap<Bytes, Vec<Bytes>>,
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

impl Store {
    pub fn new() -> Self {
        Store {
            keys: HashMap::new(),
            lists: HashMap::new(),
        }
    }

    pub fn rpush(&mut self, key: Bytes, mut values: Vec<Bytes>) -> Result<usize, StoreError> {
        let list = self.lists.entry(key).or_default();
        list.append(&mut values);
        Ok(list.len())
    }

    pub fn lpush(&mut self, key: Bytes, mut values: Vec<Bytes>) -> Result<usize, StoreError> {
        let list = self.lists.entry(key).or_default();
        values.reverse(); // reverse the order of the values
        list.splice(0..0, values); //  inserts all the values at the beginning of the list
        Ok(list.len())
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
}
#[test]
fn test_lpush() {
    let mut store = Store::new();
    let key = BytesMut::from("test").freeze();
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
