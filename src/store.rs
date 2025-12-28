use std::{
    collections::HashMap,
    ops::Deref,
    sync::Arc,
    time::{SystemTime, SystemTimeError, UNIX_EPOCH},
};

use tokio::sync::RwLock;

pub struct StringWithExpiry {
    value: String,
    expires: Option<u128>,
}

pub struct Store {
    keys: HashMap<String, StringWithExpiry>,
    lists: HashMap<String, Vec<String>>,
}

#[derive(Debug)]
pub enum StoreError {
    KeyNotFound,
    KeyExpired,
    TimeError,
}

pub type SharedStore = Arc<RwLock<Store>>;

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

    pub fn rpush(&mut self, key: &str, mut values: Vec<String>) -> Result<usize, StoreError> {
        let list = self.lists.entry(key.to_string()).or_default();
        list.append(&mut values);
        Ok(list.len())
    }

    pub fn lpush(&mut self, key: &str, mut values: Vec<String>) -> Result<usize, StoreError> {
        let list = self.lists.entry(key.to_string()).or_default();
        values.reverse(); // reverse the order of the values
        list.splice(0..0, values); //  inserts all the values at the beginning of the list
        Ok(list.len())
    }

    pub fn get(&self, key: &str) -> Result<String, StoreError> {
        let result = self.keys.get(key).ok_or(StoreError::KeyNotFound)?;
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
        key: &str,
        mut start: i128,
        mut end: i128,
    ) -> Result<Vec<String>, StoreError> {
        let list = self.lists.get(key).ok_or(StoreError::KeyNotFound)?;
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
        key: &str,
        value: &str,
        expiry: Option<u128>,
    ) -> Result<(), StoreError> {
        let mut expires: Option<u128> = None;
        if let Some(expiry) = expiry {
            let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
            expires = Some(now + expiry);
        }

        let key_value = StringWithExpiry {
            value: value.to_string(),
            expires,
        };
        self.keys.insert(key.to_string(), key_value);
        Ok(())
    }

    pub fn llen(&mut self, key: &str) -> Result<usize, StoreError> {
        let list = self.lists.entry(key.to_string()).or_default();
        Ok(list.len())
    }

    pub fn lpop(&mut self, key: &str, amount: i128) -> Result<Vec<String>, StoreError> {
        let list = self.lists.entry(key.to_string()).or_default();

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

    let _ = store.lpush("test", vec!["c".into(), "b".into(), "a".into()]);

    let result = store.lrange("test", 0, -1).unwrap();
    assert_eq!(
        result,
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );

    let _ = store.lpush("test", vec!["d".into()]);
    let result = store.lrange("test", 0, -1).unwrap();
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
