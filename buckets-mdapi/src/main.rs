// Copyright 2020 Joyent, Inc.

use std::default::Default;
use std::fs;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use clap::{crate_name, crate_version};
use crossbeam_channel::bounded;
use slog::{crit, error, info, o, Drain, LevelFilter, Logger};
use tokio::net::TcpListener;
use tokio::prelude::*;
use tokio::runtime;

use fast_rpc::server;
use rocksdb::{Options, DB};

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
    let metrics = buckets_mdapi::metrics::register_metrics(&config.metrics);
    let metrics_clone = metrics.clone();
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
                metrics_clone,
                &metrics_log,
            )
        })
        .unwrap_or_else(|e| {
            crit!(log, "failed to start metrics server"; "err" => %e);
            std::process::exit(1);
        });

    // Enumerate vnodes for the shard
    let mut vnodes: Vec<u64> = Vec::new();
    let vnode_dir_iter = fs::read_dir(config.database.path).unwrap_or_else(|e| {
        crit!(log, "failed to read the vnode database directory"; "err" => %e);
        std::process::exit(1);
    });

    for entry in vnode_dir_iter {
        if let Ok(e) = entry {
            let path = e.path();
            if path.is_dir() {
                if let Some(path_str) = path.to_str() {
                    if let Ok(vnode) = u64::from_str_radix(path_str, 10) {
                        vnodes.push(vnode)
                    }
                }
            }
        }
    }

    // TODO: Create bounded channel and spawn thread for each vnode
    // let send_channel_map: HashMap<u64,
    // for v in vnodes.iter() {
    //     // Create a bounded channel with a 500 message cap
    //     let (s, r) = bounded(500);

    // }
    // TODO: Create Hashmap<vnode, Send>

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
            let metrics_clone = metrics.clone();
            let task_log = log.new(o!(
                "component" => "FastServer",
                "thread" => buckets_mdapi::util::get_thread_name()));
            let task = server::make_task(
                socket,
                move |a, c| {
                    buckets_mdapi::util::handle_msg(
                        a,
                        &pool_clone,
                        &metrics_clone,
                        c,
                    )
                },
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
