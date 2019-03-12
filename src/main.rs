/*
 * Copyright 2019 Joyent, Inc.
 */

mod bucket;
mod metrics;
mod object;
mod opts;

use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::thread;

use clap::{crate_version, value_t};
use r2d2::Pool;
use r2d2_postgres::{TlsMode, PostgresConnectionManager};
use serde_json::Value;
use slog::{Drain, Level, LevelFilter, Logger, o};
use tokio::net::TcpListener;
use tokio::prelude::*;
use rust_fast::protocol::FastMessage;
use rust_fast::server;
use slog::{error, info, trace};

static APP: &'static str = "buckets-demo";

fn other_error(msg: &str) -> Error {
    Error::new(ErrorKind::Other, String::from(msg))
}

fn msg_handler(msg: &FastMessage,
               pool: &Pool<PostgresConnectionManager>,
               log: &Logger) -> Result<Vec<FastMessage>, Error> {
    let response: Vec<FastMessage> = vec![];

    metrics::INCOMING_REQUEST_COUNTER.inc();

    match msg.data.d {
        Value::Array(ref args) => {
            match msg.data.m.name.as_str() {
                "getobject"    => object::get_handler(msg.id, &args, response, &pool, &log),
                "putobject"    => object::put_handler(msg.id, &args, response, &pool, &log),
                "deleteobject" => object::delete_handler(msg.id, &args, response, &pool, &log),
                "listobjects"  => object::list_handler(msg.id, &args, response, &pool, &log),
                "getbucket"    => bucket::get_handler(msg.id, &args, response, &pool, &log),
                "putbucket"    => bucket::put_handler(msg.id, &args, response, &pool, &log),
                "deletebucket" => bucket::delete_handler(msg.id, &args, response, &pool, &log),
                "listbuckets"  => bucket::list_handler(msg.id, &args, response, &pool, &log),
                _ => Err(Error::new(ErrorKind::Other, format!("Unsupported functon: {}", msg.data.m.name)))
            }
        }
        _ => Err(other_error("Expected JSON array"))
    }
}

fn main() {
    let matches = opts::parse(APP.to_string());

    let pg_url = matches.value_of("pg_url")
        .unwrap_or("postgresql://postgres@localhost:5432/test");
    let listen_address = matches.value_of("address")
        .unwrap_or("127.0.0.1");
    let listen_port = value_t!(matches, "port", u32)
        .unwrap_or(2030);
    let metrics_address_str = matches.value_of("metrics-address")
        .unwrap_or("0.0.0.0");
    let metrics_port = value_t!(matches, "metrics-port", u32)
        .unwrap_or(3020);

    let level = matches.value_of("level").unwrap_or("info");

    let filter_level = match level.parse::<Level>() {
        Ok(filter_level) => filter_level,
        Err(_) => {
            println!("invalid log level: {}", level);
            std::process::exit(1);
        }
    };

    let root_log = Logger::root(
        Mutex::new(LevelFilter::new(
            slog_bunyan::default(
                std::io::stdout()
            ),
            filter_level
        )).fuse(),
        o!("build-id" => crate_version!())
    );

    // Configure and start metrics server
    let metrics_log = root_log.clone();
    let metrics_address = metrics_address_str.to_owned();
    thread::spawn(move || metrics::start_server(metrics_address,
                                                metrics_port,
                                                metrics_log));

    trace!(root_log, "postgres connection pool url: {}", pg_url);
    info!(root_log, "establishing postgres connection pool");
    let manager = PostgresConnectionManager::new(pg_url, TlsMode::None)
        .expect("Failed to create pg connection manager");
    let pool = Pool::new(manager).expect("Failed to create pg connection pool");
    info!(root_log, "established postgres connection pool");

    let addr = [listen_address, &":", &listen_port.to_string()].concat();
    let addr = addr.parse::<SocketAddr>().unwrap();

    let listener = TcpListener::bind(&addr).expect("failed to bind");
    info!(root_log, "listening for fast requests"; "address" => addr);

    tokio::run({
        let process_log = root_log.clone();
        let err_log = root_log.clone();
        listener.incoming()
            .map_err(move |e| {
                error!(&err_log, "failed to accept socket"; "err" => %e)
            })
            .for_each(
                move |socket| {

                    let pool_clone = pool.clone();
                    server::process(socket,
                                    Arc::new(move |a, c| msg_handler(a, &pool_clone, c)),
                                    &process_log);
                    Ok(())
                })
    });
}
