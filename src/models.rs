use crate::error::Error::*;
use crate::error::Result;
use redis::{aio::Connection, Commands, Connection as SyncConnection};

#[allow(dead_code)]
pub async fn get_con(client: &redis::Client) -> Result<Connection> {
    client
        .get_async_connection()
        .await
        .map_err(RedisClientError)
}

pub fn get_sync_con(client: &redis::Client) -> Result<SyncConnection> {
    client.get_connection().map_err(RedisClientError)
}

pub fn set(con: &mut SyncConnection, key: &str, value: &str) -> Result<String> {
    match con.set(key, value) {
        Ok(res) => Ok(res),
        Err(e) => Err(RedisCMDError(e)),
    }
}

pub fn get(con: &mut SyncConnection, key: &str) -> Result<Option<String>> {
    match con.get(key) {
        Ok(val) => Ok(val),
        Err(e) => Err(RedisCMDError(e)),
    }
}

/**
 * Set the TTL of given key
 */
pub fn expire(con: &mut SyncConnection, key: &str, ttl: usize) -> Result<i32> {
    match con.expire(key, ttl) {
        Ok(res) => Ok(res),
        Err(e) => Err(RedisCMDError(e)),
    }
}
