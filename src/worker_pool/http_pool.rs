use crate::worker_pool::cache;
use anyhow::Result;
use async_channel;
use reqwest::{self, StatusCode};
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, oneshot, Mutex, RwLock};
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
    url: String,                             // attachment url to download
    path: String,                            // path to store payload in
    result: oneshot::Sender<HttpPoolResult>, // channel to receive status
}

/*
 * HttpPoolResponse
 * Received by the client requesting work
*/
pub struct HttpPoolResponse {
    url: String,
    status: StatusCode,
    file: String,
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
                        cmd = command_rx.recv() => {
                            match cmd {
                                _shutdown => {
                                    println!("HttpPoolWorker[{i}]: Received shutdown signal");
                                    break;
                                }
                            }
                        },
                        task = task_rx.recv() => {
                            match task {
                                Ok(request) => {
                                    if request.result.is_closed() {
                                        println!(
                                            "HttpPoolWorker[{i}]: Received closed request for url: {}",
                                            request.url
                                        );
                                    } else {
                                        println!(
                                            "HttpPoolWorker[{i}]: Received a new request for url: {}",
                                            request.url
                                        );
                                        println!("Send request for url {}", request.url);
                                        let client = reqwest::Client::new();
                                        let res = client.get("https://google.com").send().await.unwrap();
                                        let payload = res.bytes().await.unwrap();
                                        // println!("{:?}", payload);
                                        // let _ = reqwest::get("https://httpbin.org/ip").await;
                                        println!("Done Send request for url {}", request.url);

                                        println!("Done request for url {}", request.url);
                                        match request.result.send(HttpPoolResult{}) {
                                                Ok(_) => println!("Send back request results to caller"),
                                                Err(_) => println!("Failed to send back the request to the caller, properly already run into a timeout")
                                            }
                                    }
                            },
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

    pub async fn get(&self, url: String, timeout: u64) -> Result<HttpPoolResponse> {
        // check cache
        let key = "someKey".to_owned();
        if let Some(cache) = &self.cache {
            if cache.exists(&key, None).await {}
        }
        println!("Key not found, fetch from backend");

        let (os_sender, os_receiver) = oneshot::channel::<HttpPoolResult>();

        let request = HttpPoolRequest {
            path: "".to_owned(),
            result: os_sender,
            url: url.clone(),
        };

        self.task_tx
            .send(request)
            .await
            .expect("Failed to publish message to task group");

        // check if a timeout or value was returned
        match time::timeout(time::Duration::from_millis(timeout), os_receiver).await {
            Ok(res) => {
                println!("Request for url {url} finished without reaching the timeout");
                match res {
                    Ok(res) => {
                        println!("Request for url {url} receive message via result channel");
                        // println!("{res}");
                        // match res {
                        //     Ok(res) => {
                        //         println!(
                        //             "Request for url {req_url} returned status code {}",
                        //             res.status()
                        //         );
                        //     }
                        //     Err(err) => {
                        //         println!("Request for url {req_url} failed with error {err}");
                        //     }
                        // }
                        Ok(HttpPoolResponse {
                            file: String::from("Test").to_owned(),
                            status: StatusCode::ACCEPTED,
                            url: String::from("Test").to_owned(),
                        })
                    }
                    Err(err) => {
                        println!("Request for url {url} failed to receive message {err}");
                        Ok(HttpPoolResponse {
                            file: String::from("Test").to_owned(),
                            status: StatusCode::ACCEPTED,
                            url: String::from("Test").to_owned(),
                        })
                    }
                }
            }
            Err(err) => {
                println!("Request for url {url} run into timeout");
                Ok(HttpPoolResponse {
                    file: String::from("Test").to_owned(),
                    status: StatusCode::ACCEPTED,
                    url: String::from("Test").to_owned(),
                })
            }
        }
    }

    pub async fn shutdown(mut self) -> Result<(), Box<dyn Error>> {
        if let Err(_) = self.command_tx.send(HttpPoolCommand::Shutdown) {
            return Err("Failed".to_owned().into());
        }
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
