// Copyright 2019 Joyent, Inc.

use std::io::Error;
use std::net::TcpStream;
use std::process;
use std::sync::Mutex;

use clap::{crate_version, App, Arg, ArgMatches};
use cmd_lib::{run_fun, FunResult};
use serde_json::{json, Value};
use slog::{crit, error, info, o, warn, Drain, Logger};

use cueball::connection_pool::types::ConnectionPoolOptions;
use cueball::connection_pool::ConnectionPool;
use cueball_manatee_primary_resolver::ManateePrimaryResolver;
use cueball_postgres_connection::{PostgresConnection, PostgresConnectionConfig};
use rust_fast::client as fast_client;
use rust_fast::protocol::{FastMessage, FastMessageId};
use sapi::{ZoneConfig, SAPI};

use utils::config;
use utils::schema;

/* TODO
    - make boray config file, schema file and fast arg
      command line args for overriding.
*/

const APP: &str = "schema-manager";
const BORAY_CONFIG_FILE_PATH: &str = "/opt/smartdc/boray/etc/config.toml";
const TEMPLATE_DIR: &str = "/opt/smartdc/boray/schema_templates";
const DEFAULT_EB_PORT: u32 = 2020;

// Get the boray config on the local boray zone
fn get_boray_config() -> config::Config {
    config::read_file(BORAY_CONFIG_FILE_PATH)
}

// Get the sapi URL from the local zone
fn get_sapi_url() -> FunResult {
    run_fun!("/usr/sbin/mdata-get SAPI_URL")
}

// Call out to the sapi endpoint and get this zone's configuration
fn get_zone_config(sapi: &SAPI) -> Result<ZoneConfig, Box<dyn std::error::Error>> {
    let zone_uuid = get_zone_uuid()?;
    sapi.get_zone_config(&zone_uuid)
}

// Get the boray zone UUID for use in the sapi config request
fn get_zone_uuid() -> FunResult {
    run_fun!("/usr/bin/zonename")
}

// Get a sapi client from the URL in the zone config
fn init_sapi_client(sapi_address: &str, log: &Logger) -> Result<SAPI, Error> {
    Ok(SAPI::new(&sapi_address, 60, log.clone()))
}

// Iterated through the vnodes returned from electric-boray and create the
// associated schemas on the shards
fn parse_vnodes(
    conn: &mut PostgresConnection,
    log: &Logger,
    msg: &FastMessage,
) -> Result<(), Error> {
    let v: Vec<Value> = msg.data.d.as_array().unwrap().to_vec();
    for vnode in v {
        for v in vnode.as_array().unwrap() {
            let vn = v.as_str().unwrap();
            info!(log, "processing vnode: {:#?}", vn);
            schema::create_bucket_schemas(conn, TEMPLATE_DIR, vn, log)?;
        }
    }
    Ok(())
}

// Not really used for anything yet
fn parse_opts<'a>(app: String) -> ArgMatches<'a> {
    App::new(app)
        .version(crate_version!())
        .about("Tool to manage postgres schemas for boray")
        .arg(
            Arg::with_name("fast_args")
                .help("JSON-encoded arguments for RPC method call")
                .long("args")
                .takes_value(true)
                .required(false),
        )
        .get_matches()
}

// Callback function handed in to the fast::recieve call.
fn vnode_response_handler(
    conn: &mut PostgresConnection,
    log: &Logger,
    msg: &FastMessage,
) -> Result<(), Error> {
    match msg.data.m.name.as_str() {
        "getvnodes" => {
            parse_vnodes(conn, log, msg)?;
        }
        _ => warn!(log, "Received unrecognized {} response", msg.data.m.name),
    }

    Ok(())
}

// Do the deed
fn run(log: &Logger) -> Result<(), Box<dyn std::error::Error>> {
    let sapi_url = get_sapi_url()?;
    info!(log, "sapi_url:{}", sapi_url);
    let sapi = init_sapi_client(&sapi_url, &log)?;
    let zone_config = get_zone_config(&sapi)?;
    let eb_address = zone_config.metadata.electric_boray;
    let boray_host = zone_config.metadata.service_name;
    let boray_config = get_boray_config();
    let boray_port = boray_config.server.port;

    let fast_arg = [
        String::from("tcp://"),
        boray_host,
        String::from(":"),
        boray_port.to_string(),
    ]
    .concat();

    info!(log, "pnode argument to electric-boray:{}", fast_arg);
    let eb_endpoint = [eb_address, String::from(":"), DEFAULT_EB_PORT.to_string()].concat();
    info!(log, "electric-boray endpoint:{}", eb_endpoint);
    let mut stream = TcpStream::connect(&eb_endpoint).unwrap_or_else(|e| {
        error!(log, "failed to connect to electric-boray: {}", e);
        process::exit(1)
    });

    let tls_config = utils::config::tls::tls_config(
        boray_config.database.tls_mode,
        boray_config.database.certificate,
    )
    .unwrap_or_else(|e| {
        crit!(log, "TLS configuration error"; "err" => %e);
        std::process::exit(1);
    });

    // Create a connection pool using the admin user
    let pg_config = PostgresConnectionConfig {
        user: Some(boray_config.database.user),
        password: None,
        host: None,
        port: None,
        database: Some(boray_config.database.database),
        application_name: Some(boray_config.database.application_name),
        tls_config,
    };

    let connection_creator = PostgresConnection::connection_creator(pg_config);

    let pool_opts = ConnectionPoolOptions {
        max_connections: Some(boray_config.cueball.max_connections),
        claim_timeout: Some(10000),
        log: Some(log.new(o!(
            "component" => "CueballConnectionPool"
        ))),
        rebalancer_action_delay: boray_config.cueball.rebalancer_action_delay,
        decoherence_interval: None,
    };

    let resolver = ManateePrimaryResolver::new(
        boray_config.zookeeper.connection_string,
        boray_config.zookeeper.path,
        Some(log.new(o!(
            "component" => "ManateePrimaryResolver"
        ))),
    );

    let pool = ConnectionPool::new(pool_opts, resolver, connection_creator);

    let mut conn = pool
        .claim()
        .expect("failed to acquire postgres connection for vnode schema setup");

    let mut msg_id = FastMessageId::new();
    let recv_cb = |msg: &FastMessage| vnode_response_handler(&mut conn, &log, msg);
    let vnode_method = String::from("getvnodes");

    fast_client::send(vnode_method, json!([fast_arg]), &mut msg_id, &mut stream)
        .and_then(|_| fast_client::receive(&mut stream, recv_cb))?;
    info!(log, "Done.");
    Ok(())
}

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let log = Logger::root(
        Mutex::new(slog_term::FullFormat::new(plain).build()).fuse(),
        o!("build-id" => "0.1.0"),
    );

    let _options = parse_opts(APP.to_string());

    run(&log)
}
