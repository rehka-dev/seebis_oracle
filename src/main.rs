mod worker_pool;

use std::sync::Arc;

use actix_web::{
    get,
    web::{self, BytesMut},
    App, HttpServer, Responder,
};
use tokio::fs::File;
use worker_pool::http_pool::HttpPool;

use tokio::io::{AsyncReadExt, AsyncSeekExt};
// use tokio::{fs::File, sync::RwLock};

struct AppState {
    http_pool: Arc<HttpPool>,
}

// const PAGE_SIZE: usize = 4096;

#[get("/hello/{name}")]
async fn greet(name: web::Path<String>, data: web::Data<AppState>) -> impl Responder {
    let offset: usize = 0; // received from the API call
    let next = String::from(name.as_str()).parse::<usize>().unwrap();
    let timeout: u64 = 10000;

    // request it
    match &data
        .http_pool
        .get(
            "https://google.com".to_owned(),
            &String::from("google"),
            timeout,
            offset,
            next,
        )
        .await
    {
        Ok(_) => {
            println!("Some response received")
        }
        Err(err) => println!("Err: {:?}", err),
    }

    // read data
    let mut buffer = Vec::<u8>::with_capacity(next);
    match &data
        .http_pool
        .cache
        .read_data(&String::from("google"), 0, &mut buffer)
        .await
    {
        Ok(size) => println!("Read {size} byets"),
        Err(err) => println!("HTTP Handler failed to read the data {}", err),
    }

    // release data

    format!("Payload {:?}!", String::from_utf8_lossy(&buffer))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let http_pool = worker_pool::http_pool::HttpPool::builder()
        .size(20)
        .timeout(1200)
        .use_local_cache("/home/akarner/Desktop/edi_poc/".to_owned())
        .build();
    http_pool.start().expect("Failed to start http pool");

    let http_pool_arc = Arc::new(http_pool);

    // let sql_pool

    let app_data = web::Data::new(AppState {
        http_pool: http_pool_arc.clone(),
    });

    let _ = HttpServer::new(move || App::new().app_data(app_data.clone()).service(greet))
        .bind(("127.0.0.1", 8080))
        .expect("Failed to start WebServer properly")
        .run()
        .await;

    if let Ok(http_pool) = Arc::try_unwrap(http_pool_arc) {
        println!("Http poll unwrapped successfully");
        if let Err(err) = http_pool.shutdown().await {
            println!("Failed to shutdown pool with err {err}")
        }
    }

    Ok(())
}
