// Copyright 2019 Joyent, Inc.
use std::collections::HashMap;
use std::fs;
use std::io::{Error, ErrorKind};
use std::net::TcpStream;
use std::process;

use clap::{Arg, App, crate_version, ArgMatches};
use cmd_lib::{run_fun, FunResult};
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use rust_fast::client as fast_client;
use rust_fast::protocol::{FastMessage, FastMessageId};
use sapi::{SAPI, ZoneConfig};
use serde::Serialize;
use serde_json::{Value, json};
use slog::{error, info, o, Drain, Logger};
use std::sync::Mutex;
use string_template::Template;
use utils::config;

/* TODO
    - populate service name per shard
    - make boray config file, schema file and fast arg
      command line args for overriding.
*/

static APP: &'static str = "schema-manager";
static BORAY_CONFIG_FILE_PATH: &'static str = "/opt/smartdc/boray/etc/config.toml";
const DEFAULT_EB_PORT: u32 = 2020;
const SCHEMA_STR: &'static str = "/opt/smartdc/boray/schema_templates/schema.in";
const ADMIN_STR: &'static str = "/opt/smartdc/boray/schema_templates/admin.in";
const DB_STR: &'static str = "/opt/smartdc/boray/schema_templates/db.in";

#[derive(Clone, Debug, Serialize)]
pub struct MethodOptions {
    pub req_id: String, // UUID as string,
}

// create users, role, database and schemas
fn create_bucket_schemas(log: &Logger, vnode: &str) -> Result<(), Error> {
    let admin_url = format_admin_db_url();
    let schema_template = read_schema_template()?;
    let template = Template::new(&schema_template);
    let mut args = HashMap::new();
    args.insert("vnode", vnode);
    let schema_str = template.render(&args);

    info!(log, "Creating boray user and role if they don't exist on {}", admin_url);
    match create_user_role(&admin_url) {
        Ok(_) => {
            info!(log, "Creating boray database if it doesn't exist on {}", admin_url);
            match create_database(&admin_url) {
                Ok(_) => {
                    let boray_url = format_boray_db_url();
                    let boray_conn = establish_db_connection(&boray_url);
                    info!(log, "Creating boray schemas on {}", boray_url);
                    match boray_conn.batch_execute(&schema_str) {
                        Ok(_) => Ok(()),
                        Err(e) => {
                            error!(log, "error on schema creation:{}, vnode:{}", e.to_string(), vnode);
                            Err(std::io::Error::new(ErrorKind::Other, e))
                        }
                    }
                },
                Err(e) => {
                    info!(log, "error on database creation:{}", e.to_string());
                    Err(std::io::Error::new(ErrorKind::Other, e))
                }
            }
        },
        Err(e) => {
            info!(log, "error on role creation:{}", e.to_string());
            Err(std::io::Error::new(ErrorKind::Other, e))
        }
    }
}

// create the db in its own transaction
fn create_database(db_url: &str) -> Result<(), Error> {
    let conn = establish_db_connection(&db_url);
    let db_str = read_db_template()?;
    match conn.batch_execute(&db_str) {
        Ok(_) => Ok(()),
        Err(e) => {
            if e.to_string().contains("already exists") {
                return Ok(())
            } else {
               Err(std::io::Error::new(ErrorKind::Other, e))
            }
        }
    }
}

// create the role in its own transaction
fn create_user_role(db_url: &str) -> Result<(), Error> {
    let conn = establish_db_connection(&db_url);
    let admin_str = read_admin_template()?;
    match conn.batch_execute(&admin_str) {
        Ok(_) => Ok(()),
        Err(e) => {
            if e.to_string().contains("already exists") {
                return Ok(())
            } else {
               Err(std::io::Error::new(ErrorKind::Other, e))
            }
        }
    }
}

// This uses SimpleConnection, which is discouraged, but we need to
// run batch_execute, only available there.
fn establish_db_connection(database_url: &str) -> PgConnection {
    PgConnection::establish(database_url)
        .expect(&format!("Error connecting to {}", database_url))
}

// For use connecting to the shard Postgres server
fn format_admin_db_url() -> String {
    let boray_config = get_boray_config();
    let db_url = format!("postgres://{}:{}@{}:{}",
                             boray_config.database.admin_user,
                             boray_config.database.admin_user,
                             boray_config.database.host,
                             boray_config.database.port
                         );
    db_url
}

// For use connecting to the shard Postgres server
fn format_boray_db_url() -> String {
    let boray_config = get_boray_config();
    let db_url = format!("postgres://{}:{}@{}:{}/{}",
                             boray_config.database.user,
                             boray_config.database.user,
                             boray_config.database.host,
                             boray_config.database.port,
                             boray_config.database.database
                         );
    db_url
}

// Get the boray config on the local boray zone
fn get_boray_config() -> utils::config::Config {
    config::read_file(BORAY_CONFIG_FILE_PATH)
}

// Get the sapi URL from the local zone
fn get_sapi_url() -> FunResult {
    run_fun!("/usr/sbin/mdata-get SAPI_URL")
}

// Call out to the sapi endpoint and get this zone's configuration
fn get_zone_config(sapi: SAPI) -> Result<ZoneConfig, Box<dyn std::error::Error>> {
    let zone_uuid = get_zone_uuid()?;
    sapi.get_zone_config(&zone_uuid)
}

// Get the boray zone UUID for use in the sapi config request
fn get_zone_uuid() -> FunResult {
    run_fun!("/usr/bin/zonename")
}

// Get a sapi client from the URL in the zone config
fn init_sapi_client(sapi_address: String, log: Logger) -> Result<SAPI, Error> {
    Ok(SAPI::new(&sapi_address, 60, log.clone()))
}

// Iterated through the vnodes returned from electric-boray and create the
// associated schemas on the shards
fn parse_vnodes(log: &Logger, msg: &FastMessage) -> Result<(), Error> {
    let v: Vec<Value> = msg.data.d.as_array().unwrap().to_vec();
    for vnode in v {
        for v in vnode.as_array().unwrap() {
            let vn = v.as_str().unwrap();
            info!(log, "processing vnode: {:#?}", vn);
            create_bucket_schemas(log, v.as_str().unwrap())?;
        }
    }
    Ok(())
}

// Not really used for anything yet
fn parse_opts<'a, 'b>(app: String) -> ArgMatches<'a> {
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

// Schema template is stored in boray/schema_templates/schema.in
fn read_schema_template() -> Result<String, Error> {
    fs::read_to_string(SCHEMA_STR)
}

// Admin template is stored in boray/schema_templates/admin.in
fn read_admin_template() -> Result<String, Error> {
    fs::read_to_string(ADMIN_STR)
}

// db template is stored in boray/schema_templates/db.in
fn read_db_template() -> Result<String, Error> {
    fs::read_to_string(DB_STR)
}

// Callback function handed in to the fast::recieve call.
fn vnode_response_handler(log: &Logger, msg: &FastMessage) -> Result<(), Error> {
    match msg.data.m.name.as_str() {
        "getvnodes" => {
            parse_vnodes(log, msg)?;
        }
        _ => info!(log, "Received unrecognized {} response", msg.data.m.name),
    }

    Ok(())
}

// Do the deed
fn run(
    log: Logger) -> Result<(), Box<dyn std::error::Error>>
{
    let sapi_url = get_sapi_url()?;
    info!(log, "sapi_url:{}", sapi_url);
    let sapi = init_sapi_client(sapi_url, log.clone())?;
    let zone_config = get_zone_config(sapi)?;
    let eb_address = zone_config.metadata.electric_boray;
    let boray_host = zone_config.metadata.service_name;
    let boray_config = get_boray_config();
    let boray_port = boray_config.server.port;

    let fast_arg = [String::from("tcp://"), boray_host, String::from(":"), boray_port.to_string()]
                    .concat();
    info!(log, "pnode argument to electric-boray:{}", fast_arg);
    let eb_endpoint = [eb_address, String::from(":"), DEFAULT_EB_PORT.to_string()]
        .concat();
    info!(log, "electric-boray endpoint:{}", eb_endpoint);
    let mut stream = TcpStream::connect(&eb_endpoint).unwrap_or_else(|e| {
        error!(log, "failed to connect to electric-boray: {}", e);
        process::exit(1)
    });

    let mut msg_id = FastMessageId::new();
    let recv_cb = |msg: &FastMessage| { vnode_response_handler(&log, msg) };
    let vnode_method = String::from("getvnodes");

    fast_client::send(vnode_method,
                      json!([fast_arg]),
                      &mut msg_id,
                      &mut stream).and_then(
            |_| fast_client::receive(&mut stream, recv_cb),
    )?;
    Ok(())
}

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let log = Logger::root(
        Mutex::new(slog_term::FullFormat::new(plain).build()).fuse(),
                   o!("build-id" => "0.1.0"),
    );

    let _options = parse_opts(APP.to_string());

    run(log)?;
    println!("Done.");
    Ok(())
}
