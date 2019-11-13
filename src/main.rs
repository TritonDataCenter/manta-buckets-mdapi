// Copyright 2019 Joyent, Inc.

pub mod config;

use std::default::Default;
use std::net::{IpAddr, SocketAddr};
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use clap::{crate_name, crate_version};
use slog::{error, info, o, Drain, LevelFilter, Logger};
use tokio::net::TcpListener;
use tokio::prelude::*;
use tokio::runtime;

use cueball::connection_pool::types::ConnectionPoolOptions;
use cueball::connection_pool::ConnectionPool;
use cueball_postgres_connection::{PostgresConnection, PostgresConnectionConfig};
use cueball_static_resolver::StaticIpResolver;
use rust_fast::server;

use config::Config;

fn main() {
    let matches = boray::opts::parse(crate_name!());

    // Optionally read config file
    let mut config: Config = match matches.value_of("config") {
        Some(f) => config::read_file(f),
        None => config::Config::default(),
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
    let metrics_thread_builder = thread::Builder::new()
        .name("metrics-server".into());
    let m = log.clone();
    let _mtb_handler = metrics_thread_builder.spawn(move || {
        let metrics_log = m.new(o!(
            "component" => "MetricsServer",
            "thread" => boray::util::get_thread_name()
        ));
        boray::metrics::start_server(&metrics_host, metrics_port, &metrics_log)
    });

    let tls_config = config::tls::tls_config(config.database.tls_mode, config.database.certificate)
        .unwrap_or_else(|e| {
            error!(log, "TLS configuration error"; "err" => %e);
            std::process::exit(1);
        });

    let pg_config = PostgresConnectionConfig {
        user: Some(config.database.user),
        password: None,
        host: Some(config.database.host),
        port: Some(config.database.port),
        database: Some(config.database.database),
        application_name: Some(config.database.application_name),
        tls_config,
    };

    let connection_creator = PostgresConnection::connection_creator(pg_config);

    // When the manatee resolver is complete then this will need to log the
    // dynamic backend in some other way.  For now, at least reporting backend
    // ip/port will be helpful.
    let pool_opts = ConnectionPoolOptions {
        maximum: config.cueball.max_connections,
        claim_timeout: config.cueball.claim_timeout,
        log: log.new(o!(
            "component" => "CueballConnectionPool",
            "backend_ip" => pg_ip.to_string(),
            "backend_port" => config.database.port,
        )),
        rebalancer_action_delay: config.cueball.rebalancer_action_delay,
    };

    let primary_backend = (pg_ip, config.database.port);
    let resolver = StaticIpResolver::new(vec![primary_backend]);

    let pool = ConnectionPool::new(pool_opts, resolver, connection_creator);

    let addr = [&config.server.host, ":", &config.server.port.to_string()].concat();
    let addr = addr.parse::<SocketAddr>().unwrap();

    let listener = TcpListener::bind(&addr).expect("failed to bind");
    info!(log, "listening"; "address" => addr);

    let l = log.clone();
    let server = listener
        .incoming()
        .map_err(move |e| error!(log.clone(), "failed to accept socket"; "err" => %e))
        .for_each(move |socket| {
            let pool_clone = pool.clone();
            let task_log = l.new(o!(
                "component" => "FastServer",
                "thread" => boray::util::get_thread_name()));
            let task = server::make_task(
                socket,
                move |a, c| boray::util::handle_msg(a, &pool_clone, c),
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
