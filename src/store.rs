use std::{
    collections::HashMap,
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
    data: HashMap<String, StringWithExpiry>,
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
            data: HashMap::new(),
        }
    }

    pub fn get(&self, key: &str) -> Result<String, RespParseError> {
        let result = self.data.get(key).ok_or(RespParseError::KeyNotFound)?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();

        if let Some(expiry) = result.expires {
            if expiry < now {
                return Err(RespParseError::KeyExpired);
            }
        }

        Ok(result.value.clone())
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
        self.data.insert(key.to_string(), key_value);
        Ok(())
    }
}
