use crate::worker_pool::cache;
use anyhow::Result;
use async_channel;
use reqwest::{self};
use std::error::Error;
use std::fmt;
use std::path::PathBuf;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time;

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
    url: String,                               // attachment url to download
    path: PathBuf,                             // path to store payload in
    result: oneshot::Sender<HttpPoolResponse>, // channel to receive status
}

pub enum HttpPoolResponse {
    Success,
    Failed,
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
    cache: Option<Box<dyn cache::HttpPoolCache + Sync + Send>>,

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
                        cmd_msg = command_rx.recv() => {
                            match cmd_msg {
                                Ok(cmd) => {
                                    match cmd {
                                        HttpPoolCommand::Shutdown => {
                                            println!("HttpPoolWorker[{i}]: Received shutdown signal");
                                            break;
                                        }
                                        _ => {
                                            println!("HttpWorker[{i}]: Received some unknown commmand");
                                        }
                                    }

                                },
                                Err(_) => println!("HttpWorker[{i}]: Failed to receive some cmd"),
                                }
                            },
                        task = task_rx.recv() => {
                            match task {
                                Ok(req) => {
                                    if req.result.is_closed() {
                                        println!(
                                            "HttpPoolWorker[{i}]: Received closed request for url: {}",
                                            req.url
                                        );
                                    } else {
                                        println!(
                                            "HttpPoolWorker[{i}]: Received a new request for url: {}",
                                            req.url
                                        );
                                        println!("Send request for url {}", req.url);
                                        let client = reqwest::Client::new();
                                        let mut result = HttpPoolResponse::Success;
                                        if let Err(_) = client.get(req.url).send().await {
                                           result = HttpPoolResponse::Failed;
                                        }

                                        // let mut file = File::create(req.)

                                        match req.result.send(result) {
                                            Ok(_) => println!("Send back request results to caller"),
                                            Err(_) => println!("Failed to send back the request to the caller, properly already run into a timeout")
                                        }

                                        }


                                        // let payload = res.bytes().await.unwrap();
                                        // // println!("{:?}", payload);
                                        // // let _ = reqwest::get("https://httpbin.org/ip").await;
                                        // println!("Done Send request for url {}", request.url);

                                        // println!("Done request for url {}", request.url);
                                        // match request.result.send(HttpPoolResponse{}) {
                                        //         Ok(_) => println!("Send back request results to caller"),
                                        //         Err(_) => println!("Failed to send back the request to the caller, properly already run into a timeout")
                                        //     }
                                    }
                            Err(err) => {
                                println!("Received error {err} during channel reading a new task")
                            }
                        }
                        }
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
        if let Some(cache) = &self.cache {
            if cache.exists(key, Some(next + off)).await {
                println!("Request can be served by the cache, let's return success");
                return Ok(HttpPoolResponse::Success);
            }
            println!("Request cannot be served by the cache");
            // add a new entry to the cache
            cache_file = cache.add(key).await?;
        }

        println!("Load data from backend via the http pool workers");
        let (os_sender, os_receiver) = oneshot::channel::<HttpPoolResponse>();
        let request = HttpPoolRequest {
            path: PathBuf::from(cache_file),
            result: os_sender,
            url: url.clone(),
        };

        self.task_tx.send(request).await?;
        let channel_res = time::timeout(time::Duration::from_millis(timeout), os_receiver).await?;
        let _http_res = channel_res?;
        println!("Http Pool finished request {url} with some status");
        Ok(HttpPoolResponse::Success)
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
            cache: self.cache,

            task_tx: task_tx,
            task_rx: task_rx,
            command_tx: command_tx,
            command_rx: command_rx,
            shutdown_tx: shutdown_tx,
            shutdown_rx: shutdown_rx,
        }
    }
}
