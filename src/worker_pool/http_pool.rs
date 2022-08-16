use crate::worker_pool::cache::{self, CacheUpdate};
use anyhow::Result;
use async_channel;
use futures_util::StreamExt;
use reqwest::{self};
use std::error::Error;
use std::fmt;
use std::path::PathBuf;
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
    cache: Box<dyn cache::HttpPoolCache + Sync + Send>,

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

            tokio::spawn(async move {
                let _shutdown_sender = shutdown_tx;
                loop {
                    tokio::select! {
                        task = task_rx.recv() => {
                            let cache = &self.cache;
                            if let Ok(req) = task {
                                println!("HttpPoolWorker[{i}]: Received a new request for url: {} and cache file {:?}", req.url, req.path);
                                let client = reqwest::Client::new();
                                if let Ok(res) = client.get(req.url).send().await {
                                    // TODO: use abstraction via cache trait
                                    if let Ok(mut file) = File::create(req.path).await {
                                        let mut stream = res.bytes_stream();
                                        let mut size :usize = 0;
                                        while let Some(item) = stream.next().await {
                                            println!("Store next chunk of data");
                                            if let Ok(data) = item {
                                                if let Err(_) = file.write_all(&data).await {
                                                    println!("Failed to write next chunk of data");
                                                    break;
                                                }
                                                size += data.len();
                                                // if let Err(_) = self.cache.set_size(&req.key, size, false).await {
                                                //     println!("Failed to update cache entry");
                                                // }
                                            }
                                        }
                                        // if let Err(_) = self.cache.set_size(&req.key, size, true).await {
                                        //     println!("Failed to update cache entry");
                                        // }
                                    } else {
                                        println!("HttpPoolWorker[{i}]: Failed to open cache storage");
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
        if self.cache.exists(key, Some(next + off)).await {
            println!("Request can be served by the cache, let's return success");
            let res = self.cache.read_response(key).await?;
            return Ok(HttpPoolResponse::CacheHit(res));
        }

        println!("Request cannot be served by the cache");
        // add a new entry to the cache and subscribe to it notify channel
        cache_file = self.cache.add(key).await?;
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
                    if (off + next) >= size {
                        break;
                    }
                }
                Ok(CacheUpdate::Finished) => {
                    break;
                }
                Err(err) => {
                    // TODO: check if we are now done
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
            cache: self.cache.unwrap(),

            task_tx: task_tx,
            task_rx: task_rx,
            command_tx: command_tx,
            command_rx: command_rx,
            shutdown_tx: shutdown_tx,
            shutdown_rx: shutdown_rx,
        }
    }
}
