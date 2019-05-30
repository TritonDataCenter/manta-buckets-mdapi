/*
 * Copyright 2019 Joyent, Inc.
 */

pub mod config;

use std::net::{IpAddr, SocketAddr};
use std::sync::Mutex;
use std::fs;
use std::thread;

use clap::{crate_version, crate_name, value_t};
use cueball::connection_pool::ConnectionPool;
use cueball::connection_pool::types::ConnectionPoolOptions;
use cueball_static_resolver::StaticIpResolver;
use cueball_postgres_connection::{PostgresConnection, PostgresConnectionConfig};
use slog::{Drain, LevelFilter, Logger, o};
use tokio::net::TcpListener;
use tokio::prelude::*;
use rust_fast::server;
use slog::{error, info};

use config::Config;

fn main() {
    let matches = boray::opts::parse(crate_name!());

    /*
     * Default configuration parameters are set here.  These can first be overridden by a config
     * file specified with `-c <config>`, and then overridden again by specific command line
     * arguments.
     */
    let mut level: String = "info".to_owned();
    let mut host: String = "127.0.0.1".to_owned();
    let mut port: u16 = 2030;
    let mut pg_host: String = "127.0.0.1".to_owned();
    let mut pg_port: u16 = 5432;
    let mut pg_db: String = "moray".to_owned();
    let mut metrics_host: String = "0.0.0.0".to_owned();
    let mut metrics_port: u16 = 3020;

    // Optionally read config file
    match matches.value_of("config") {
        Some(f) => {
            let s = match fs::read(f) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("Failed to read config file: {}", e);
                    std::process::exit(1);
                }
            };

            let config: Config = match toml::from_slice(&s) {
                Ok(config) => config,
                Err(e) => {
                    eprintln!("Failed to parse config file: {}", e);
                    std::process::exit(1);
                }
            };

            level = config.log.level;

            host = config.server.host;
            port = config.server.port;

            pg_host = config.database.host;
            pg_port = config.database.port;
            pg_db = config.database.db;

            metrics_host = config.metrics.host;
            metrics_port = config.metrics.port;
        },
        None => {}
    }

    // Read CLI arguments
    level = matches.value_of("level").map_or(level, std::borrow::ToOwned::to_owned);

    host = matches.value_of("address").map_or(host, std::borrow::ToOwned::to_owned);
    port = value_t!(matches, "port", u16).unwrap_or(port);

    pg_host = matches.value_of("pg ip").map_or(pg_host, std::borrow::ToOwned::to_owned);
    pg_port = value_t!(matches, "pg port", u16).unwrap_or(pg_port);
    pg_db = matches.value_of("pg database").map_or(pg_db, std::borrow::ToOwned::to_owned);

    metrics_host = matches.value_of("metrics-address").map_or(metrics_host, std::borrow::ToOwned::to_owned);
    metrics_port = value_t!(matches, "metrics-port", u16).unwrap_or(metrics_port);

    // XXX postgres host must be an IP address currently
    let pg_ip: IpAddr = match pg_host.parse() {
        Ok(pg_ip) => pg_ip,
        Err(e) => {
            eprintln!("postgres host MUST be an IPv4 address: {}", e);
            std::process::exit(1);
        }
    };

    let filter_level = match level.parse() {
        Ok(filter_level) => filter_level,
        Err(_) => {
            eprintln!("invalid log level: {}", level);
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
    thread::spawn(move || boray::metrics::start_server(metrics_host,
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

    let addr = [&host, ":", &port.to_string()].concat();
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
