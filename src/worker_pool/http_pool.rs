use crate::worker_pool::cache::{self, CacheResult, CacheUpdate};
use anyhow::Result;
use async_channel;
use futures_util::StreamExt;
use reqwest::{self};
use std::error::Error;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::File;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time;

use tokio::io::AsyncWriteExt;

/*
 * HttpPoolResult
 * Internal struct holding the information about the
 * request result
 */
#[derive(Debug)]
struct HttpPoolResult {}

impl fmt::Display for HttpPoolResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "HttpPoolResult")
    }
}
/*
 * HttpPoolRequest
 * Requested by the clients and hand over to the pool to
 * load and store the payload properly
 */
pub struct HttpPoolRequest {
    key: String,
    url: String,   // attachment url to download
    path: PathBuf, // path to store payload in
}

pub enum HttpPoolResponse {
    CacheHit(cache::HttpResponse),
    CacheMiss(cache::HttpResponse),
}

/* HttpPoolCommand
* Broadcasted to all workers to request some action
*/
#[derive(Clone)]
pub enum HttpPoolCommand {
    Shutdown,
}
pub struct HttpPool {
    size: i32,
    timeout: i32,
    pub cache: Arc<Box<dyn cache::HttpPoolCache + Sync + Send>>,

    // channels to distribute work and shutdown graceful
    task_tx: async_channel::Sender<HttpPoolRequest>,
    task_rx: async_channel::Receiver<HttpPoolRequest>,
    command_tx: broadcast::Sender<HttpPoolCommand>,
    command_rx: broadcast::Receiver<HttpPoolCommand>,
    shutdown_tx: mpsc::Sender<()>,
    shutdown_rx: mpsc::Receiver<()>,
}

impl HttpPool {
    pub fn builder() -> HttpPoolBuilder {
        HttpPoolBuilder::new()
    }

    pub fn start(&self) -> Result<(), Box<dyn Error>> {
        for i in 0..self.size {
            let task_rx = self.task_rx.clone();
            let mut command_rx = self.command_tx.subscribe();
            let shutdown_tx = self.shutdown_tx.clone();
            let cache = self.cache.clone();

            tokio::spawn(async move {
                let _shutdown_sender = shutdown_tx;
                let cache = cache;
                loop {
                    tokio::select! {
                        task = task_rx.recv() => {
                            if let Ok(req) = task {
                                println!("HttpPoolWorker[{i}]: Received a new request for url: {} and cache file {:?}", req.url, req.path);
                                let client = reqwest::Client::new();
                                if let Ok(res) = client.get(req.url).send().await {
                                    if false == cache.set_response(&req.key, cache::HttpResponse{
                                        status: res.status().as_u16()
                                    }).await {
                                        println!("Failed to set the response properly");
                                    }
                                    // TODO: use abstraction via cache trait
                                    match File::create(req.path).await {
                                        Ok(mut file) => {
                                            let mut stream = res.bytes_stream();
                                            let mut size :usize = 0;
                                            while let Some(item) = stream.next().await {
                                                println!("Store next chunk of data");
                                                if let Ok(data) = item {
                                                    if let Err(_) = file.write_all(&data).await {
                                                        println!("Failed to write next chunk of data");
                                                        break;
                                                    }
                                                    if let Err(_) = file.flush().await {
                                                        println!("Failed to flush next chunk of data");
                                                        break;
                                                    }
                                                    if let Err(_) = file.sync_data().await {
                                                        println!("Failed to sync chunk data");
                                                        break;
                                                    }
                                                    //use tokio::time::{sleep, Duration};
                                                    //sleep(Duration::from_millis(500)).await;
                                                    size += data.len();
                                                    if let Err(_) = cache.set_size(&req.key, size, false).await {
                                                        println!("Failed to update cache entry");
                                                    }
                                                }
                                            }
                                        },
                                        Err(err) => println!("Failed to update cache entry {}", err),
                                    }
                                } else {
                                    println!("HttpPoolWorker[{i}]: Request failed to execute");
                                }
                            } else {
                                // TODO: should we shutdown?
                                println!("HttpPoolWorker[{i}]: Failed to receive new task via channel");
                            }
                        }
                        cmd = command_rx.recv() => {
                            match cmd {
                                Ok(HttpPoolCommand::Shutdown) => {
                                    println!("HttpPoolWorker[{i}]: Received shutdown signal");
                                    break;
                                },
                                // Ok(_) =>  println!("HttpWorker[{i}]: Received some unknown commmand"),
                                Err(_)=> println!("HttpWorker[{i}]: Failed to receive some cmd"),
                            }
                            },
                    }
                }
                println!("HttpPoolWorker[{i}]: Shutdown gracefully");
            });
            println!("Spawned new http task with id[{i}]");
        }
        Ok(())
    }

    pub async fn get(
        &self,
        url: String,
        key: &String,
        timeout: u64,
        off: usize,
        next: usize,
    ) -> Result<HttpPoolResponse> {
        println!("Received http-pool request for key {key} and url {url}");

        // TODO: use some tmpfs file in case no caching is enabled
        let mut cache_file = String::from("");
        match self.cache.exists(key, Some(next + off)).await {
            CacheResult::Present => {
                println!("Request can be served by the cache, let's return success");
                let res = self.cache.read_response(key).await?;
                return Ok(HttpPoolResponse::CacheHit(res));
            }
            CacheResult::NotFound => {
                println!("Request cannot be served by the cache");
                // add a new entry to the cache and subscribe to it notify channel
                cache_file = self.cache.add(key).await?;
            }
            CacheResult::Running => {
                println!("Request currently running");
            }
        }

        let mut notify_rx = self.cache.subscribe(key).await?;

        println!("Load data from backend via the http pool workers");
        self.task_tx
            .send(HttpPoolRequest {
                key: key.clone(),
                path: PathBuf::from(cache_file),
                url: url.clone(),
            })
            .await?;

        loop {
            println!("Start to wait for the worker to finish loading the needed data");
            match notify_rx.recv().await {
                Ok(CacheUpdate::Update(size)) => {
                    // check if we have already received enough data
                    println!("Received runing update: {size} and need {}", off + next);
                    if size >= (off + next) {
                        break;
                    }
                }
                Ok(CacheUpdate::Finished) => {
                    println!("Received finished update");
                    break;
                }
                Err(err) => {
                    // TODO: check if we are now done and therefore the channel is closed
                    return Err(anyhow::Error::from(err));
                }
            }
        }
        println!("Http Pool finished request {url} with some status");
        let res = self.cache.read_response(key).await?;
        return Ok(HttpPoolResponse::CacheMiss(res));
    }

    pub async fn shutdown(mut self) -> Result<()> {
        self.command_tx.send(HttpPoolCommand::Shutdown);
        drop(self.shutdown_tx);

        let _ = self.shutdown_rx.recv().await;
        return Ok(());
    }
}

/*---------------------------------------------------------------------------*/
/*
 * Builder for HttpPool instance
 */
pub struct HttpPoolBuilder {
    size: i32,
    timeout: i32,
    cache: Option<Box<dyn cache::HttpPoolCache + Sync + Send>>,
}

/*
 * HttpPool builder implementation
 */
impl HttpPoolBuilder {
    pub fn new() -> HttpPoolBuilder {
        HttpPoolBuilder {
            size: 10,
            timeout: 60,
            cache: None,
        }
    }

    pub fn size(mut self, size: i32) -> HttpPoolBuilder {
        self.size = size;
        self
    }

    pub fn timeout(mut self, timeout: i32) -> HttpPoolBuilder {
        self.timeout = timeout;
        self
    }

    pub fn cache(mut self, cache: Box<dyn cache::HttpPoolCache + Sync + Send>) -> HttpPoolBuilder {
        self.cache = Some(cache);
        self
    }

    pub fn build(self) -> HttpPool {
        let (task_tx, task_rx) = async_channel::bounded::<HttpPoolRequest>(self.size as usize);
        let (command_tx, command_rx) = broadcast::channel::<HttpPoolCommand>(self.size as usize);
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        HttpPool {
            size: self.size,
            timeout: self.timeout,
            cache: Arc::new(self.cache.unwrap()),

            task_tx: task_tx,
            task_rx: task_rx,
            command_tx: command_tx,
            command_rx: command_rx,
            shutdown_tx: shutdown_tx,
            shutdown_rx: shutdown_rx,
        }
    }
}
