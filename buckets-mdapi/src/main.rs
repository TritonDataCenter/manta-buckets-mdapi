// Copyright 2019 Joyent, Inc.

use std::default::Default;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use clap::{crate_name, crate_version};
use slog::{crit, error, info, o, Drain, LevelFilter, Logger};
use tokio::net::TcpListener;
use tokio::prelude::*;
use tokio::runtime;

use cueball::connection_pool::types::ConnectionPoolOptions;
use cueball::connection_pool::ConnectionPool;
use cueball_manatee_primary_resolver::ManateePrimaryResolver;
use cueball_postgres_connection::{
    PostgresConnection, PostgresConnectionConfig,
};
use rust_fast::server;

use utils::config::Config;

fn main() {
    let matches = buckets_mdapi::opts::parse(crate_name!());

    // Optionally read config file
    let mut config: Config = match matches.value_of("config") {
        Some(f) => utils::config::read_file(f),
        None => utils::config::Config::default(),
    };

    // Read CLI arguments
    utils::config::read_cli_args(&matches, &mut config);

    let log = Logger::root(
        Mutex::new(LevelFilter::new(
            slog_bunyan::with_name(crate_name!(), std::io::stdout()).build(),
            config.log.level.into(),
        ))
        .fuse(),
        o!("v" => crate_version!()),
    );

    // Configure and start metrics server
    let metrics_host = config.metrics.host.clone();
    let metrics_port = config.metrics.port;
    let metrics_thread_builder =
        thread::Builder::new().name("metrics-server".into());
    let m = log.clone();
    let _mtb_handler = metrics_thread_builder
        .spawn(move || {
            let metrics_log = m.new(o!(
                "component" => "MetricsServer",
                "thread" => buckets_mdapi::util::get_thread_name()
            ));
            buckets_mdapi::metrics::start_server(
                &metrics_host,
                metrics_port,
                &metrics_log,
            )
        })
        .unwrap_or_else(|e| {
            crit!(log, "failed to start metrics server"; "err" => %e);
            std::process::exit(1);
        });

    let tls_config = utils::config::tls::tls_config(
        config.database.tls_mode,
        config.database.certificate,
    )
    .unwrap_or_else(|e| {
        crit!(log, "TLS configuration error"; "err" => %e);
        std::process::exit(1);
    });

    let pg_config = PostgresConnectionConfig {
        user: Some(config.database.user),
        password: None,
        host: None,
        port: None,
        database: Some(config.database.database),
        application_name: Some(config.database.application_name),
        tls_config,
    };

    let connection_creator = PostgresConnection::connection_creator(pg_config);

    //
    // TODO log the dynamic backend IP somehow? The resolver will at least emit
    // log entries when the IP changes.
    //
    let pool_opts = ConnectionPoolOptions {
        max_connections: Some(config.cueball.max_connections),
        claim_timeout: config.cueball.claim_timeout,
        log: Some(log.new(o!(
            "component" => "CueballConnectionPool"
        ))),
        rebalancer_action_delay: config.cueball.rebalancer_action_delay,
        decoherence_interval: None,
    };

    let resolver = ManateePrimaryResolver::new(
        config.zookeeper.connection_string,
        config.zookeeper.path,
        Some(log.new(o!(
            "component" => "ManateePrimaryResolver"
        ))),
    );

    let pool = ConnectionPool::new(pool_opts, resolver, connection_creator);

    info!(log, "established postgres connection pool");

    let addr =
        [&config.server.host, ":", &config.server.port.to_string()].concat();
    let addr = addr.parse::<SocketAddr>().unwrap();

    let listener = TcpListener::bind(&addr).expect("failed to bind");
    info!(log, "listening"; "address" => addr);

    let err_log = log.clone();
    let server = listener
        .incoming()
        .map_err(
            move |e| error!(&err_log, "failed to accept socket"; "err" => %e),
        )
        .for_each(move |socket| {
            let pool_clone = pool.clone();
            let task_log = log.new(o!(
                "component" => "FastServer",
                "thread" => buckets_mdapi::util::get_thread_name()));
            let task = server::make_task(
                socket,
                move |a, c| buckets_mdapi::util::handle_msg(a, &pool_clone, c),
                Some(&task_log),
            );
            tokio::spawn(task);
            Ok(())
        });

    let mut rt = runtime::Builder::new()
        .blocking_threads(config.tokio.blocking_threads)
        .core_threads(config.tokio.core_threads.unwrap())
        .keep_alive(config.tokio.thread_keep_alive.map(Duration::from_secs))
        .name_prefix(config.tokio.thread_name_prefix)
        .stack_size(config.tokio.thread_stack_size)
        .build()
        .unwrap();

    rt.spawn(server);

    // Wait until the runtime becomes idle and shut it down.
    rt.shutdown_on_idle().wait().unwrap();
}
