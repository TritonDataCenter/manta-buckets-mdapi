/*
 * Copyright 2019 Joyent, Inc.
 */

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Mutex;
use std::thread;

use clap::{crate_version, crate_name, value_t};
use cueball::connection_pool::ConnectionPool;
use cueball::connection_pool::types::ConnectionPoolOptions;
use cueball_static_resolver::StaticIpResolver;
use cueball_postgres_connection::{PostgresConnection, PostgresConnectionConfig};
use slog::{Drain, Level, LevelFilter, Logger, o};
use tokio::net::TcpListener;
use tokio::prelude::*;
use rust_fast::server;
use slog::{error, info};

fn main() {
    let matches = boray::opts::parse(crate_name!());

    let pg_ip = value_t!(matches, "pg ip", IpAddr)
        .unwrap_or_else(|_| IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
    let pg_port = value_t!(matches, "pg port", u16)
        .unwrap_or(5432);
    let pg_db = matches.value_of("pg database")
        .unwrap_or("moray");
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
    thread::spawn(move || boray::metrics::start_server(metrics_address,
                                                       metrics_port,
                                                       metrics_log));

    info!(root_log, "establishing postgres connection pool");
    let user = "postgres";
    let application_name = "boray";
    let pg_config = PostgresConnectionConfig {
        user: Some(user.into()),
        password: None,
        host: Some(pg_ip.to_string()),
        port: Some(pg_port.into()),
        database: Some(pg_db.into()),
        application_name: Some(application_name.into())
    };

    let connection_creator = PostgresConnection::connection_creator(pg_config);
    let pool_opts = ConnectionPoolOptions {
        maximum: 5,
        claim_timeout: None,
        log: root_log.clone(),
        rebalancer_action_delay: None
    };

    let primary_backend = (pg_ip, pg_port);
    let resolver = StaticIpResolver::new(vec![primary_backend]);

    let pool = ConnectionPool::new(
        pool_opts,
        resolver,
        connection_creator
    );

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
                    let task = server::make_task(
                        socket,
                        move |a, c| boray::util::msg_handler(a, &pool_clone, c),
                        &process_log,
                    );
                    tokio::spawn(task);
                    Ok(())
                })
    });
}
