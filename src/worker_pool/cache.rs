use async_trait::async_trait;
use chrono::{DateTime, Local, Utc};
use std::collections::HashMap;
use tokio::sync::RwLock;

use super::http_pool::HttpPoolBuilder;

/*
 * CacheEntry struct holding information
 */
#[derive(Debug)]
pub struct CacheEntry {
    present: bool,                  // present bit
    path: String,                   // path to local file
    created: chrono::DateTime<Utc>, // created timestamp
    ref_count: u32,                 // usage counter
    size: u64,
}

/*
 * HttpPoolCache instance trait
 */
#[async_trait]
pub trait HttpPoolCache {
    async fn entry_exists(&self, key: &String, size: Option<u64>) -> bool;
    async fn get_stream(&self, key: &String) -> Option<CacheEntry>;
}

/*
 * Simple file based cache instance
 * Implements the HttpPoolCache instance
 */
pub struct LocalCache {
    storage: String,
    info: RwLock<HashMap<String, CacheEntry>>,
}

impl LocalCache {
    fn new() -> Self {
        LocalCache {
            storage: "".to_owned(),
            info: RwLock::new(HashMap::<String, CacheEntry>::new()),
        }
    }
}

/*
 * Extend the HttpPoolBuilder to allow using LocalCache instance
 */
impl HttpPoolBuilder {
    pub fn use_local_cache(self) -> HttpPoolBuilder {
        self.cache(Box::new(LocalCache::new()))
    }
}

/*
 * Function to verify, if the requested CacheEntry is available to return
 * The entry is available either if the present flag is set,
 * signalling that the entire payload is stored on disk,
 * or if we have already enough data available by setting the asize parameter
 */
#[async_trait]
impl HttpPoolCache for LocalCache {
    async fn entry_exists(&self, key: &String, asize: Option<u64>) -> bool {
        if let Some(entry) = self.info.read().await.get(key) {
            if entry.present {
                true
            } else {
                if let Some(size) = asize {
                    entry.size >= size
                } else {
                    false
                }
            }
        } else {
            false
        }
    }

    async fn get_stream(&self, key: &String) -> Option<CacheEntry> {
        if let Some(entry) = self.info.read().await.get(key) {
            Some(CacheEntry {
                present: entry.present,
                path: entry.path.clone(),
                created: entry.created,
                ref_count: entry.ref_count,
                size: entry.size,
            })
        } else {
            None
        }
    }
}
