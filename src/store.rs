use std::{
    collections::HashMap,
    ops::Deref,
    sync::Arc,
    time::{SystemTime, SystemTimeError, UNIX_EPOCH},
};

use tokio::sync::RwLock;

use crate::parser::RespParseError;

pub struct StringWithExpiry {
    value: String,
    expires: Option<u128>,
}

pub struct Store {
    keys: HashMap<String, StringWithExpiry>,
    lists: HashMap<String, Vec<String>>,
}

pub type SharedStore = Arc<RwLock<Store>>;

// TODO: overhaul error handling
impl From<SystemTimeError> for RespParseError {
    fn from(_err: SystemTimeError) -> Self {
        RespParseError::InvalidFormat
    }
}

impl Store {
    pub fn new() -> Self {
        Store {
            keys: HashMap::new(),
            lists: HashMap::new(),
        }
    }

    pub fn rpush(&mut self, key: &str, mut values: Vec<String>) -> Result<usize, RespParseError> {
        let list = self.lists.entry(key.to_string()).or_default();
        list.append(&mut values);
        Ok(list.len())
    }

    pub fn get(&self, key: &str) -> Result<String, RespParseError> {
        let result = self.keys.get(key).ok_or(RespParseError::KeyNotFound)?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();

        if let Some(expiry) = result.expires {
            if expiry < now {
                return Err(RespParseError::KeyExpired);
            }
        }

        Ok(result.value.clone())
    }

    pub fn lrange(
        &self,
        key: &str,
        start: usize,
        mut end: usize,
    ) -> Result<Vec<String>, RespParseError> {
        let list = self.lists.get(key).ok_or(RespParseError::KeyNotFound)?;
        end = end + 1;
        if start >= list.len() {
            return Ok(vec![]);
        }

        if end >= list.len() {
            end = list.len();
        }

        if start > end {
            return Ok(vec![]);
        }

        let slice = &list.as_slice()[start..end];
        Ok(slice.to_vec())
    }

    pub fn set_with_expiry(
        &mut self,
        key: &str,
        value: &str,
        expiry: Option<u128>,
    ) -> Result<(), RespParseError> {
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
}
