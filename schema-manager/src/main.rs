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
    - make config file, schema file and fast arg
      command line args for overriding.
*/

const APP: &str = "schema-manager";
const BUCKETS_MDAPI_CONFIG_FILE_PATH: &str = "/opt/smartdc/buckets-mdapi/etc/config.toml";
const TEMPLATE_DIR: &str = "/opt/smartdc/buckets-mdapi/schema_templates";
const DEFAULT_EB_PORT: u32 = 2020;

// Get the config on the local buckets-mdapi zone
fn get_buckets_mdapi_config() -> config::Config {
    config::read_file(BUCKETS_MDAPI_CONFIG_FILE_PATH)
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

// Get the buckets-mdapi zone UUID for use in the sapi config request
fn get_zone_uuid() -> FunResult {
    run_fun!("/usr/bin/zonename")
}

// Get a sapi client from the URL in the zone config
fn init_sapi_client(sapi_address: &str, log: &Logger) -> Result<SAPI, Error> {
    Ok(SAPI::new(&sapi_address, 60, log.clone()))
}

// Iterate through the vnodes returned from buckets-mdplacement and create the
// associated schemas on the shards
fn parse_vnodes(
    conn: &mut PostgresConnection,
    database_config: &config::ConfigDatabase,
    zk_config: &config::ConfigZookeeper,
    log: &Logger,
    msg: &FastMessage,
) -> Result<(), Error> {
    let v: Vec<Value> = msg.data.d.as_array().unwrap().to_vec();
    for vnode in v {
        for v in vnode.as_array().unwrap() {
            let resolver = ManateePrimaryResolver::new(
                zk_config.connection_string.clone(),
                zk_config.path.clone(),
                Some(log.new(o!(
                    "component" => "ManateePrimaryResolver"
                ))),
            );
            let vn = v.as_str().unwrap();
            info!(log, "processing vnode: {}", vn);
            schema::create_bucket_schemas(conn, database_config, resolver, TEMPLATE_DIR, vn, log)?;
        }
    }
    Ok(())
}

// Not really used for anything yet
fn parse_opts<'a>(app: String) -> ArgMatches<'a> {
    App::new(app)
        .version(crate_version!())
        .about("Tool to manage postgres schemas for buckets-mdapi")
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
    database_config: &config::ConfigDatabase,
    zk_config: &config::ConfigZookeeper,
    log: &Logger,
    msg: &FastMessage,
) -> Result<(), Error> {
    match msg.data.m.name.as_str() {
        "getvnodes" => {
            parse_vnodes(conn, database_config, zk_config, log, msg)?;
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
    let mdplacement_address = zone_config.metadata.electric_boray;
    let buckets_mdapi_host = zone_config.metadata.service_name;
    let buckets_mdapi_config = get_buckets_mdapi_config();
    let buckets_mdapi_port = buckets_mdapi_config.server.port;

    let fast_arg = [
        String::from("tcp://"),
        buckets_mdapi_host,
        String::from(":"),
        buckets_mdapi_port.to_string(),
    ]
    .concat();

    info!(log, "pnode argument to buckets-mdplacement:{}", fast_arg);
    let mdplacement_endpoint = [
        mdplacement_address,
        String::from(":"),
        DEFAULT_EB_PORT.to_string(),
    ]
    .concat();
    info!(log, "buckets-mdplacement endpoint:{}", mdplacement_endpoint);
    let mut stream = TcpStream::connect(&mdplacement_endpoint).unwrap_or_else(|e| {
        error!(log, "failed to connect to buckets-mdplacement: {}", e);
        process::exit(1)
    });

    let tls_config = utils::config::tls::tls_config(
        buckets_mdapi_config.database.tls_mode.clone(),
        buckets_mdapi_config.database.certificate.clone(),
    )
    .unwrap_or_else(|e| {
        crit!(log, "TLS configuration error"; "err" => %e);
        std::process::exit(1);
    });

    // Create a connection pool using the admin user
    let pg_config = PostgresConnectionConfig {
        user: Some(buckets_mdapi_config.database.admin_user.clone()),
        password: None,
        host: None,
        port: None,
        database: Some("postgres".into()),
        application_name: Some("schema-manager".into()),
        tls_config,
    };

    let connection_creator = PostgresConnection::connection_creator(pg_config);

    let pool_opts = ConnectionPoolOptions {
        max_connections: Some(1),
        claim_timeout: Some(10000),
        log: Some(log.new(o!(
            "component" => "CueballConnectionPool"
        ))),
        rebalancer_action_delay: buckets_mdapi_config.cueball.rebalancer_action_delay,
        decoherence_interval: None,
    };

    let resolver = ManateePrimaryResolver::new(
        buckets_mdapi_config.zookeeper.connection_string.clone(),
        buckets_mdapi_config.zookeeper.path.clone(),
        Some(log.new(o!(
            "component" => "ManateePrimaryResolver"
        ))),
    );

    let pool = ConnectionPool::new(pool_opts, resolver, connection_creator);

    let mut conn = pool
        .claim()
        .expect("failed to acquire postgres connection for vnode schema setup");

    let mut msg_id = FastMessageId::new();
    let recv_cb = |msg: &FastMessage| {
        vnode_response_handler(
            &mut conn,
            &buckets_mdapi_config.database,
            &buckets_mdapi_config.zookeeper,
            &log,
            msg,
        )
    };

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
