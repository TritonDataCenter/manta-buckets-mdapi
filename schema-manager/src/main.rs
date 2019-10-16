// Copyright 2019 Joyent, Inc.
use std::collections::HashMap;
use std::fs;
use std::io::{Error, ErrorKind};
use std::net::{SocketAddr, TcpStream};
use std::process;

use clap::{Arg, App, value_t, crate_version, ArgMatches};
use cmd_lib::{output, FunResult};
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use rust_fast::client as fast_client;
use rust_fast::protocol::{FastMessage, FastMessageId};
use sapi::{SAPI, ZoneConfig};
use serde::Serialize;
use serde_json::{Value, json};
use slog::{o, Drain, Logger};
use std::sync::Mutex;
use string_template::Template;
use utils::config;

/* TODO
    - populate service name per shard
*/

static APP: &'static str = "schema-manager";
static BORAY_CONFIG_FILE_PATH: &'static str = "../etc/config.toml";
const DEFAULT_EB_PORT: u32 = 2020;
const SCHEMA_STR: &'static str = "../etc/schema.sql";

#[derive(Clone, Debug, Serialize)]
pub struct MethodOptions {
    pub req_id: String, // UUID as string,
}

fn init_sapi_client(sapi_address: String, log: Logger) -> Result<SAPI, Error> {
    Ok(SAPI::new(&sapi_address, 60, log.clone()))
}

fn read_schema_template() -> Result<String, Error> {
    fs::read_to_string(SCHEMA_STR)
}

fn parse_vnodes(msg: &FastMessage) -> Result<(), Error> {
    let v: Vec<Value> = msg.data.d.as_array().unwrap().to_vec();
    //println!("v: {:#?}", v);
    for vnode in v {
        for v in vnode.as_array().unwrap() {
            let vn =v.as_str().unwrap();
            println!("elem:{:#?}", vn);
            create_bucket_schemas(v.as_str().unwrap())?;
        }
    }
    Ok(())
}

fn create_bucket_schemas(vnode: &str) -> Result<(), Error> {
    let db_url = format_db_url();
    let conn = establish_db_connection(db_url);
    let schema_template = read_schema_template()?;
    let template = Template::new(&schema_template);
    let mut args = HashMap::new();
    args.insert("vnode", vnode);
    let exec_str = template.render(&args);

    match conn.batch_execute(&exec_str) {
        Ok(_) => Ok(()),
        Err(e) => Err(std::io::Error::new(ErrorKind::Other, e))
    }
}

fn format_db_url() -> String {
    let boray_config = config::read_file(BORAY_CONFIG_FILE_PATH);
    let db_url = format!("postgres://{}:{}@{}:{}/{}",
                         boray_config.database.user,
                         boray_config.database.user,
                         boray_config.database.host,
                         boray_config.database.port,
                         boray_config.database.database
                         );
    db_url
}

fn establish_db_connection(database_url: String) -> PgConnection {

    PgConnection::establish(&database_url)
        .expect(&format!("Error connecting to {}", database_url))
}

fn parse_opts<'a, 'b>(app: String) -> ArgMatches<'a> {
   App::new(app)
        .version(crate_version!())
        .about("Tool to manage postgres schemas for boray")
        .arg(
            Arg::with_name("fast_args")
                .help("JSON-encoded arguments for RPC method call")
                .long("args")
                .takes_value(true)
                .required(true),
        )
        .get_matches()
}

fn vnode_response_handler(msg: &FastMessage) -> Result<(), Error> {
    match msg.data.m.name.as_str() {
        "getvnodes" => {
            parse_vnodes(msg)?;
        }
        _ => println!("Received unrecognized {} response", msg.data.m.name),
    }

    Ok(())
}

fn run(
    args: Value,
    log: Logger) -> Result<(), Box<dyn std::error::Error>>
{
    let sapi_url = get_sapi_url()?;
    let sapi = init_sapi_client(sapi_url, log.clone())?;
    let zone_config = get_zone_config(sapi)?;
    let eb_address = zone_config.metadata.electric_boray_hostname;

    let eb_endpoint = [eb_address, String::from(":"), DEFAULT_EB_PORT.to_string()]
        .concat()
        .parse::<SocketAddr>()
        .unwrap_or_else(|e| {
            println!("Failed to parse electric-boray address: {}", e);
            process::exit(1)
        });
    let mut stream = TcpStream::connect(&eb_endpoint).unwrap_or_else(|e| {
        println!("failed to connec to electric-boray: {}", e);
        process::exit(1)
    });

    let shards: Vec<Value> = args.as_array().unwrap().to_vec();
    let mut msg_id = FastMessageId::new();

    for s in shards {
        let vnode_method = String::from("getvnodes");
        fast_client::send(vnode_method,
                          json!([s]),
                          &mut msg_id,
                          &mut stream).and_then(
            |_| fast_client::receive(&mut stream, vnode_response_handler),
        )?;
    }
    Ok(())
}

fn get_zone_uuid() -> FunResult {
    output!("/usr/sbin/zoneadm list")
}

fn get_sapi_url() -> FunResult {
    output!("/usr/sbin/mdata-get SAPI_URL")
}

fn get_zone_config(sapi: SAPI) -> Result<ZoneConfig, Box<dyn std::error::Error>> {
    let zone_uuid = get_zone_uuid()?;
    sapi.get_zone_config(&zone_uuid)
}

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let log = Logger::root(
        Mutex::new(slog_term::FullFormat::new(plain).build()).fuse(),
                   o!("build-id" => "0.1.0"),
    );

    let options = parse_opts(APP.to_string());
    let args = value_t!(options, "fast_args", Value).unwrap_or_else(|e| e.exit());

    run(args, log)?;
    Ok(())
}
