/*
 * Copyright 2019 Joyent, Inc.
 */

pub mod config;

use std::default::Default;
use std::net::{IpAddr, SocketAddr};
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use clap::{crate_version, crate_name};
use slog::{Drain, LevelFilter, Logger, error, info, o};
use tokio::net::TcpListener;
use tokio::prelude::*;
use tokio::runtime;

use cueball::connection_pool::ConnectionPool;
use cueball::connection_pool::types::ConnectionPoolOptions;
use cueball_static_resolver::StaticIpResolver;
use cueball_postgres_connection::{
    PostgresConnection,
    PostgresConnectionConfig
};
use rust_fast::server;

use config::Config;

fn main() {
    let matches = boray::opts::parse(crate_name!());

    // Optionally read config file
    let mut config: Config =
        match matches.value_of("config") {
            Some(f) => config::read_file(f),
            None => Default::default()
        };

    // Read CLI arguments
    config::read_cli_args(&matches, &mut config);

    // XXX postgres host must be an IP address currently
    let pg_ip: IpAddr = match config.database.host.parse() {
        Ok(pg_ip) => pg_ip,
        Err(e) => {
            eprintln!("postgres host MUST be an IPv4 address: {}", e);
            std::process::exit(1);
        }
    };

    let root_log = Logger::root(
        Mutex::new(LevelFilter::new(
            slog_bunyan::default(
                std::io::stdout()
            ),
            config.log.level.into()
        )).fuse(),
        o!("build-id" => crate_version!())
    );

    // Configure and start metrics server
    let metrics_log = root_log.clone();
    let metrics_host = config.metrics.host.clone();
    let metrics_port = config.metrics.port.clone();
    thread::spawn(move || boray::metrics::start_server(metrics_host,
                                                       metrics_port,
                                                       metrics_log));

    info!(root_log, "establishing postgres connection pool");

    let tls_config =
        config::tls::tls_config(config.database.tls_mode,
                                config.database.certificate)
        .unwrap_or_else(|e| {
            eprintln!("TLS configuration error: {}", e);
            std::process::exit(1);
        });

    let pg_config = PostgresConnectionConfig {
        user: Some(config.database.user),
        password: None,
        host: Some(config.database.host),
        port: Some(config.database.port),
        database: Some(config.database.database),
        application_name: Some(config.database.application_name),
        tls_config: tls_config
    };

    let connection_creator = PostgresConnection::connection_creator(pg_config);

    let pool_opts = ConnectionPoolOptions {
        maximum: config.cueball.max_connections,
        claim_timeout: config.cueball.claim_timeout,
        log: root_log.clone(),
        rebalancer_action_delay: config.cueball.rebalancer_action_delay
    };

    let primary_backend = (pg_ip, config.database.port);
    let resolver = StaticIpResolver::new(vec![primary_backend]);

    let pool = ConnectionPool::new(
        pool_opts,
        resolver,
        connection_creator
    );

    info!(root_log, "established postgres connection pool");

    let addr = [&config.server.host,
                ":",
                &config.server.port.to_string()].concat();
    let addr = addr.parse::<SocketAddr>().unwrap();

    let listener = TcpListener::bind(&addr).expect("failed to bind");
    info!(root_log, "listening for fast requests"; "address" => addr);

    let process_log = root_log.clone();
    let err_log = root_log.clone();
    let server = listener.incoming()
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
            }
        );

    let mut rt = runtime::Builder::new()
        .blocking_threads(config.tokio.blocking_threads)
        .core_threads(config.tokio.core_threads)
        .keep_alive(Some(Duration::from_secs(config.tokio.thread_keep_alive)))
        .name_prefix(config.tokio.thread_name_prefix)
        .stack_size(config.tokio.thread_stack_size)
        .build()
        .unwrap();

    rt.spawn(server);

    // Wait until the runtime becomes idle and shut it down.
    rt.shutdown_on_idle()
        .wait().unwrap();
}
