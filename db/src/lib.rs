use std::collections::HashMap;
use std::io::Error;
use std::sync::Arc;
use tokio::sync::RwLock;
use async_trait::async_trait;

#[async_trait]
pub trait InMemoryDBTrait {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error>;

    async fn contains(&self, key: &[u8]) -> Result<bool, Error>;

    async fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Error>;

    async fn remove(&self, key: &[u8]) -> Result<(), Error>;

    async fn flush(&self) -> Result<(), Error> {
        Ok(())
    }
}

pub struct InMemoryDBStruct {
    light: bool,
    storage: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>,
}

impl InMemoryDBStruct {
    pub fn new(light: bool) -> Self {
        InMemoryDBStruct {
            light,
            storage: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl InMemoryDBTrait for InMemoryDBStruct {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        Ok(self.storage.read().await.get(key).cloned())
    }

    async fn contains(&self, key: &[u8]) -> Result<bool, Error> {
        Ok(self.storage.read().await.contains_key(key))
    }

    async fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Error> {
        self.storage.write().await.insert(key, value);
        Ok(())
    }

    async fn remove(&self, key: &[u8]) -> Result<(), Error> {
        if self.light {
            self.storage.write().await.remove(key);
        }
        Ok(())
    }

    async fn flush(&self) -> Result<(), Error> {
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_in_memory_db_get() {
        let in_memory_db = InMemoryDBStruct::new(true);
        in_memory_db
            .insert(b"test-key".to_vec(), b"test-value".to_vec())
            .await
            .unwrap();
        let value = in_memory_db.get(b"test-key").await.unwrap().unwrap();
    
        assert_eq!(value, b"test-value");
    }
}
