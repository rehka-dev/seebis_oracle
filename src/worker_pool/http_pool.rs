use async_channel;
use reqwest::{self, StatusCode};
use std::error::Error;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time;

pub type HttpPoolSender = async_channel::Sender<HttpRequest>;
pub type HttpPoolReceiver = async_channel::Receiver<HttpRequest>;
pub type HttpPoolResponse = Result<HttpResponse, Box<dyn Error>>;

type HPResult<T> = std::result::Result<T, Box<dyn Error>>;

pub struct HttpResponse {
    url: String,
    status: StatusCode,
    file: String,
}

pub struct HttpRequest {
    url: String,
    // result: Sender<HttpPoolResponse>,
    result: oneshot::Sender<String>,
}

#[derive(Clone)]
pub enum HttpPoolCommand {
    Shutdown,
}

pub struct HttpPoolBuilder {
    size: i32,
    timeout: i32,
}

impl HttpPoolBuilder {
    pub fn new() -> HttpPoolBuilder {
        HttpPoolBuilder {
            size: 10,
            timeout: 60,
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

    pub fn build(self) -> HttpPool {
        let (task_tx, task_rx) = async_channel::bounded::<HttpRequest>(self.size as usize);
        let (command_tx, command_rx) = broadcast::channel::<HttpPoolCommand>(self.size as usize);
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        HttpPool {
            size: self.size,
            timeout: self.timeout,
            task_tx: task_tx,
            task_rx: task_rx,
            command_tx: command_tx,
            _command_rx: command_rx,
            shutdown_tx: shutdown_tx,
            shutdown_rx: shutdown_rx,
        }
    }
}

pub struct HttpPool {
    size: i32,
    timeout: i32,
    task_tx: HttpPoolSender,
    task_rx: HttpPoolReceiver,
    command_tx: broadcast::Sender<HttpPoolCommand>,
    _command_rx: broadcast::Receiver<HttpPoolCommand>,
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
            let mut _command_rx = self.command_tx.subscribe();
            let shutdown_tx = self.shutdown_tx.clone();

            // let (send, mut recv) = mpsc::channel::<String>(1);
            // let shutdown_receiver = self.shutdown_rx.cl
            tokio::spawn(async move {
                let _shutdown_sender = shutdown_tx;
                loop {
                    tokio::select! {
                        cmd = _command_rx.recv() => {
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
                                        println!("{:?}", payload);
                                        // let _ = reqwest::get("https://httpbin.org/ip").await;
                                        println!("Done Send request for url {}", request.url);

                                        println!("Done request for url {}", request.url);
                                        match request.result.send(request.url) {
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

    pub async fn get(&self, url: String, timeout: u64) -> HttpPoolResponse {
        let (os_sender, os_receiver) = tokio::sync::oneshot::channel::<String>();

        let request = HttpRequest {
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
                        println!("{res}");
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
                        Ok(HttpResponse {
                            file: String::from("Test").to_owned(),
                            status: StatusCode::ACCEPTED,
                            url: String::from("Test").to_owned(),
                        })
                    }
                    Err(err) => {
                        println!("Request for url {url} failed to receive message {err}");
                        Ok(HttpResponse {
                            file: String::from("Test").to_owned(),
                            status: StatusCode::ACCEPTED,
                            url: String::from("Test").to_owned(),
                        })
                    }
                }
            }
            Err(err) => {
                println!("Request for url {url} run into timeout");
                Ok(HttpResponse {
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
