mod worker_pool;

use std::sync::Arc;

use actix_web::{get, http, web, App, HttpServer, Responder};
use anyhow::Result;
use worker_pool::http_pool::HttpPool;

struct AppState {
    http_pool: Arc<HttpPool>,
}

#[get("/hello/{name}")]
async fn greet<'a>(name: web::Path<String>, data: web::Data<AppState>) -> impl Responder {
    let http_pool = &data.http_pool;
    http_pool
        .get("http://localhost:8080".to_owned(), 1000)
        .await
        .expect("msg");
    format!("Hello {name}!")
}

#[get("/shutdown")]
async fn shutdown<'a>(data: web::Data<AppState>) -> String {
    format!("Shutdown should be done")
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let http_pool = worker_pool::http_pool::HttpPool::builder()
        .size(20)
        .timeout(1200)
        .build();
    http_pool.start().expect("Failed to start http pool");

    let http_pool_arc = Arc::new(http_pool);

    // let sql_pool

    let app_data = web::Data::new(AppState {
        http_pool: http_pool_arc.clone(),
    });

    let _ = HttpServer::new(move || {
        App::new()
            .app_data(app_data.clone())
            .service(greet)
            .service(shutdown)
    })
    .bind(("127.0.0.1", 8080))
    .expect("Failed to start webserver properly")
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
