use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::{fs::File, sync::RwLock};

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
    size: usize,
}

impl CacheEntry {
    fn new() -> Self {
        CacheEntry {
            present: false,
            path: "".to_owned(),
            created: chrono::Utc::now(),
            ref_count: 0,
            size: 0,
        }
    }
}

/*
 * HttpPoolCache instance trait
 */
#[async_trait]
pub trait HttpPoolCache {
    async fn exists(&self, key: &String, size: Option<usize>) -> bool;
    async fn add(&self, key: &String) -> Result<String>;
    async fn set_present(&self, key: &String) -> bool;
    async fn set_size(&self, key: &String, size: usize) -> bool;
    async fn inc_ref_count(&self, key: &String) -> bool;
    async fn dec_ref_count(&self, key: &String) -> bool;
    async fn read_data(&self, key: &String, off: usize, buf: &mut [u8]) -> anyhow::Result<usize>;
    // TODO: add some streaming function, if we requested a bigger chunk at ones
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
    fn new(path: String) -> Self {
        LocalCache {
            storage: path,
            info: RwLock::new(HashMap::<String, CacheEntry>::new()),
        }
    }

    async fn build_cache_path(&self, key: &String) -> Result<String> {
        let mut path = PathBuf::from(self.storage.clone());
        path.push(key);
        path.set_extension("txt");

        if let Some(res) = path.to_str() {
            Ok(res.to_owned())
        } else {
            Err(anyhow::Error::msg(
                "Failed to create cache path out of the key",
            ))
        }
    }
}

/*
 * Extend the HttpPoolBuilder to allow using LocalCache instance
 */
impl HttpPoolBuilder {
    pub fn use_local_cache(self, path: String) -> HttpPoolBuilder {
        self.cache(Box::new(LocalCache::new(path)))
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
    async fn exists(&self, key: &String, asize: Option<usize>) -> bool {
        if let Some(entry) = self.info.read().await.get(key) {
            if entry.present {
                return true;
            } else {
                if let Some(size) = asize {
                    return entry.size >= size;
                } else {
                    return false;
                }
            }
        }

        false
    }

    async fn add(&self, key: &String) -> Result<String> {
        let mut map = self.info.write().await;
        if map.contains_key(key) {
            Err(anyhow::Error::msg(format!(
                "Duplicate key in cache found for {}",
                key
            )))
        } else {
            let mut entry = CacheEntry::new();
            let path = self.build_cache_path(key).await?;
            entry.path = path.clone();

            map.insert(key.clone(), entry);
            Ok(path)
        }
    }

    async fn set_present(&self, key: &String) -> bool {
        if let Some(entry) = self.info.write().await.get_mut(key) {
            entry.present = true;
            true
        } else {
            false
        }
    }

    async fn set_size(&self, key: &String, size: usize) -> bool {
        if let Some(entry) = self.info.write().await.get_mut(key) {
            entry.size = size;
            true
        } else {
            false
        }
    }

    async fn inc_ref_count(&self, key: &String) -> bool {
        if let Some(entry) = self.info.write().await.get_mut(key) {
            entry.ref_count = std::cmp::min(entry.ref_count + 1, std::u32::MAX);
            true
        } else {
            false
        }
    }
    async fn dec_ref_count(&self, key: &String) -> bool {
        if let Some(entry) = self.info.write().await.get_mut(key) {
            entry.ref_count = std::cmp::max(entry.ref_count - 1, 0);
            true
        } else {
            false
        }
    }

    async fn read_data(&self, key: &String, off: usize, buf: &mut [u8]) -> anyhow::Result<usize> {
        if let Some(entry) = self.info.read().await.get(key) {
            let mut file = File::open(&entry.path).await?;
            // file.seek(io::SeekFrom::Start(off as u64)).await?;
            Ok(file.read(buf).await?)
        } else {
            Err(anyhow::Error::msg("Requested data from unknown cache key"))
        }
    }
}
