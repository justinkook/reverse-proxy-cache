use crate::cache::{CacheData};
use crate::error::{Error, Result};

use std::collections::HashMap;
use std::sync::Arc;
use std::vec::Vec;
use tokio::{sync::RwLock};

/// Storage is an abstraction over a persistent storage.
#[derive(Clone)]
pub enum Storage {
    Memory {
        map: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    },
}

impl Storage {
    pub async fn read(&self, name: &str) -> Result<CacheData> {
        match &self {
            Storage::Memory { map, .. } => map.read().await.get(name).map_or(
                Err(Error::IoError(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "No such key.",
                ))),
                |x| Ok(x.clone().into()),
            ),
        }
    }

    pub async fn persist(&self, name: &str, data: CacheData) {
        match self {
            Storage::Memory { ref map, .. } => {
                map.write()
                    .await
                    .insert(name.to_string(), data.into_vec_u8().await);
            }
        }
    }

    pub async fn remove(&self, name: &str) -> Result<()> {
        match self {
            Storage::Memory { map, .. } => {
                map.write().await.remove(name);
                Ok(())
            }
        }
    }
    pub fn new_mem() -> Self {
        Storage::Memory {
            map: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}
