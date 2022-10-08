use crate::error::Result;
use crate::models;
use crate::storage::Storage;
use crate::util;

use async_trait::async_trait;
use bytes::Bytes;
use futures::{future, stream, Stream, StreamExt};
use std::convert::AsRef;
use std::fmt;
use std::marker::Send;
use std::str;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::vec::Vec;

/// Datatype of cache size.
/// Note: It is persistent in some database, so changes may not be backward compatible.
pub type CacheSizeType = u64;

pub enum CacheHitMiss {
    Hit,
    Miss,
}

pub enum CacheData {
    TextData(String),
    BytesData(Bytes),
    ByteStream(
        Box<dyn Stream<Item = Result<Bytes>> + Send + Unpin>,
        Option<CacheSizeType>,
    ), // stream and size
}

impl CacheData {
    pub fn len(&self) -> CacheSizeType {
        match &self {
            CacheData::TextData(text) => text.len() as CacheSizeType,
            CacheData::BytesData(bytes) => bytes.len() as CacheSizeType,
            CacheData::ByteStream(_, size) => size.unwrap(),
        }
    }

    pub async fn into_vec_u8(self) -> Vec<u8> {
        match self {
            CacheData::TextData(text) => text.into_bytes(),
            CacheData::BytesData(bytes) => bytes.to_vec(),
            CacheData::ByteStream(stream, _) => {
                let mut vec: Vec<u8> = Vec::new();
                stream
                    .for_each(|item| {
                        vec.append(&mut item.unwrap().to_vec());
                        future::ready(())
                    })
                    .await;
                vec
            }
        }
    }

    pub fn into_byte_stream(self) -> Box<dyn Stream<Item = Result<Bytes>> + Send + Unpin> {
        match self {
            CacheData::TextData(data) => {
                let stream = stream::iter(vec![Ok(Bytes::from(data))]);
                let stream: Box<dyn Stream<Item = Result<Bytes>> + Send + Unpin> = Box::new(stream);
                stream
            }
            CacheData::BytesData(data) => {
                let stream = stream::iter(vec![Ok(data)]);
                Box::new(stream)
            }
            CacheData::ByteStream(stream, _) => stream,
        }
    }
}

impl From<String> for CacheData {
    fn from(s: String) -> CacheData {
        CacheData::TextData(s)
    }
}

impl From<Bytes> for CacheData {
    fn from(bytes: Bytes) -> CacheData {
        CacheData::BytesData(bytes)
    }
}

impl From<Vec<u8>> for CacheData {
    fn from(vec: Vec<u8>) -> CacheData {
        CacheData::BytesData(Bytes::from(vec))
    }
}

impl AsRef<[u8]> for CacheData {
    fn as_ref(&self) -> &[u8] {
        // TODO:
        match &self {
            CacheData::TextData(text) => text.as_ref(),
            CacheData::BytesData(bytes) => bytes.as_ref(),
            _ => unimplemented!(),
        }
    }
}

impl fmt::Debug for CacheData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut f = f.debug_struct("CacheData");
        match &self {
            CacheData::TextData(s) => f.field("TextData", s),
            CacheData::BytesData(b) => f.field("Bytes", b),
            CacheData::ByteStream(_, size) => f.field(
                "ByteStream",
                &format!(
                    "(stream of size {})",
                    size.map(|x| format!("{}", x))
                        .unwrap_or_else(|| "unknown".to_string())
                ),
            ),
        };
        f.finish()
    }
}

/// Cache is a trait that defines the shared beshaviors of all cache policies.
/// - `put`: put a key-value pair into the cache
/// - `get`: get a value from the cache
#[async_trait]
pub trait Cache: Sync + Send {
    async fn put(&mut self, key: &str, entry: CacheData);
    async fn get(&self, key: &str) -> Option<CacheData>;
}

/// `TtlMetadataStore` defines required behavior for a TTL cache
pub trait TtlMetadataStore: Sync + Send {
    fn get_ttl_entry(&self, key: &str) -> CacheHitMiss;
    fn set_ttl_entry(&self, key: &str, value: &CacheData, ttl: u64);
    fn spawn_expiration_cleanup_thread(
        &self,
        storage: &Storage,
        pending_close: Arc<AtomicBool>,
    ) -> Result<JoinHandle<()>>;
}

pub struct TtlCache {
    pub ttl: u64,
    metadata_db: Arc<dyn TtlMetadataStore>,
    storage: Arc<Storage>,
    pub pending_close: Arc<AtomicBool>,
    pub expiration_thread_handler: Option<JoinHandle<()>>,
}

impl TtlCache {
    pub fn new(ttl: u64, metadata_db: Arc<dyn TtlMetadataStore>, storage: Arc<Storage>) -> Self {
        let mut cache = Self {
            ttl,
            metadata_db,
            storage,
            pending_close: Arc::new(AtomicBool::new(false)),
            expiration_thread_handler: None,
        };
        let thread_handler = cache
            .metadata_db
            .spawn_expiration_cleanup_thread(&cache.storage, cache.pending_close.clone())
            .unwrap();
        cache.expiration_thread_handler = Some(thread_handler);
        cache
    }
}

#[async_trait]
impl Cache for TtlCache {
    async fn get(&self, key: &str) -> Option<CacheData> {
        match self.metadata_db.get_ttl_entry(key) {
            CacheHitMiss::Hit => {
                return match self.storage.read(key).await {
                    Ok(data) => {
                        trace!("CACHE GET [HIT] {} -> {:?} ", key, data);
                        Some(data)
                    }
                    Err(_) => None,
                };
            }
            CacheHitMiss::Miss => {
                trace!("CACHE GET [MISS] {}", key);
                None
            }
        }
    }
    async fn put(&mut self, key: &str, entry: CacheData) {
        self.metadata_db.set_ttl_entry(key, &entry, self.ttl);
        self.storage.persist(key, entry).await;
    }
}

pub struct RedisMetadataDb {
    redis_client: redis::Client,
    id: String,
}

impl RedisMetadataDb {
    pub fn new(redis_client: redis::Client, id: &str) -> Self {
        Self {
            redis_client,
            id: id.into(),
        }
    }

    pub fn get_redis_key(id: &str, cache_key: &str) -> String {
        format!("{}/{}", id, cache_key)
    }

    pub fn from_redis_key(id: &str, key: &str) -> String {
        String::from(&key[id.len() + 1..])
    }
}

impl TtlMetadataStore for RedisMetadataDb {
    fn get_ttl_entry(&self, key: &str) -> CacheHitMiss {
        let redis_key = Self::get_redis_key(&self.id, key);
        let mut sync_con = models::get_sync_con(&self.redis_client).unwrap();
        match models::get(&mut sync_con, &redis_key) {
            Ok(res) => match res {
                Some(_) => CacheHitMiss::Hit,
                None => CacheHitMiss::Miss,
            },
            Err(e) => {
                info!("get cache entry key={} failed: {}", key, e);
                CacheHitMiss::Miss
            }
        }
    }
    fn set_ttl_entry(&self, key: &str, _value: &CacheData, ttl: u64) {
        let redis_key = Self::get_redis_key(&self.id, key);
        let mut sync_con = models::get_sync_con(&self.redis_client).unwrap();
        match models::set(&mut sync_con, &redis_key, "") {
            Ok(_) => {}
            Err(e) => {
                error!("set cache entry for {} failed: {}", key, e);
            }
        }
        match models::expire(&mut sync_con, &redis_key, ttl as usize) {
            Ok(_) => {}
            Err(e) => {
                error!("set cache entry ttl for {} failed: {}", key, e);
            }
        }
        trace!("CACHE SET {} TTL={}", &key, ttl);
    }

    fn spawn_expiration_cleanup_thread(
        &self,
        storage: &Storage,
        pending_close: Arc<AtomicBool>,
    ) -> Result<JoinHandle<()>> {
        let cloned_client = self.redis_client.clone();
        let id_clone = self.id.to_string();
        let storage_clone = storage.clone();
        let pending_close_clone = pending_close;

        let expiration_thread_handler = std::thread::spawn(move || {
            debug!("TTL expiration listener is created!");
            futures::executor::block_on(async move {
                loop {
                    if pending_close_clone.load(std::sync::atomic::Ordering::SeqCst) {
                        return;
                    }
                    match cloned_client.get_connection() {
                        Ok(mut con) => {
                            let mut pubsub = con.as_pubsub();
                            trace!("subscribe to cache key pattern: {}", &id_clone);
                            match pubsub.psubscribe(format!("__keyspace*__:{}*", &id_clone)) {
                                Ok(_) => {}
                                Err(e) => {
                                    error!("Failed to psubscribe: {}", e);
                                    continue;
                                }
                            }
                            pubsub
                                .set_read_timeout(Some(std::time::Duration::from_secs(1)))
                                .unwrap();
                            loop {
                                // break if the associated cache object is about to be closed
                                if pending_close_clone.load(std::sync::atomic::Ordering::SeqCst) {
                                    return;
                                }
                                match pubsub.get_message() {
                                    Ok(msg) => {
                                        let channel: String = msg.get_channel().unwrap();
                                        let payload: String = msg.get_payload().unwrap();
                                        let redis_key = &channel[channel.find(':').unwrap() + 1..];
                                        let file = Self::from_redis_key(&id_clone, redis_key);
                                        trace!(
                                            "channel '{}': payload {}, file: {}",
                                            msg.get_channel_name(),
                                            payload,
                                            file,
                                        );
                                        if payload != "expired" {
                                            continue;
                                        }
                                        match storage_clone.remove(&file).await {
                                            Ok(_) => {
                                                info!("TTL cache removed {}", &file);
                                            }
                                            Err(e) => {
                                                warn!("Failed to remove {}: {}", &file, e);
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        if e.kind() == redis::ErrorKind::IoError && e.is_timeout() {
                                            // ignore timeout error, as expected
                                        } else {
                                            error!(
                                                "Failed to get_message, retrying every 3s: {} {:?}",
                                                e,
                                                e.kind()
                                            );
                                            util::sleep_ms(3000);
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to get redis connection: {}", e);
                            util::sleep_ms(3000);
                        }
                    }
                }
            });
        });
        Ok(expiration_thread_handler)
    }
}

impl Drop for TtlCache {
    /// The spawned key expiration handler thread needs to be dropped.
    fn drop(&mut self) {
        self.pending_close
            .store(true, std::sync::atomic::Ordering::SeqCst);
        if let Some(thread_handler) = self.expiration_thread_handler.take() {
            thread_handler.thread().unpark();
            thread_handler.join().unwrap();
            trace!("spawned thread dropped.");
        } else {
            warn!("expiration_thread_handler is None! If the thread is not spawned in the first place, the cache may have not been working properly. Otherwise, a thread is leaked.");
        }
    }
}

#[derive(Hash, Eq, PartialEq, Debug)]
pub struct CacheEntry<Metadata, Key, Value> {
    pub metadata: Metadata,
    pub key: Key,
    pub value: Value,
}

pub struct NoCache {}

#[async_trait]
impl Cache for NoCache {
    async fn put(&mut self, _key: &str, _entry: CacheData) {}
    async fn get(&self, _key: &str) -> Option<CacheData> {
        None
    }
}
