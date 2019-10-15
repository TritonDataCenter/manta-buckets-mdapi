// Copyright 2019 Joyent, Inc.
use std::io::{Error, ErrorKind};
use std::net::{SocketAddr, TcpStream};
use std::process;

use clap::{Arg, App, value_t, crate_version, ArgMatches};
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use rust_fast::client as fast_client;
use rust_fast::protocol::{FastMessage, FastMessageId};
use sapi::SAPI;
use serde::Serialize;
use serde_json::{Value, json};
use slog::{o, Drain, Logger};
use std::sync::Mutex;
use string_template::Template;
use std::collections::HashMap;
use utils::config;

static APP: &'static str = "schema-manager";
//static BORAY_CONFIG_FILE_PATH: &'static str = "/opt/smartdc/boray/etc/config.toml";
static BORAY_CONFIG_FILE_PATH: &'static str = "./config.toml";
static DEFAULT_SAPI_ADDRESS: &'static str = "10.77.77.137";
const DEFAULT_SAPI_PORT: u32 = 2030;
static DEFAULT_EB_ADDRESS: &'static str = "10.77.77.137";
const DEFAULT_EB_PORT: u32 = 2020;
const SCHEMA_STR: &'static str = include_str!("schema.sql");

#[derive(Clone, Debug, Serialize)]
pub struct MethodOptions {
    pub req_id: String, // UUID as string,
}

fn init_sapi_client(sapi_address: String, log: Logger) -> Result<SAPI, Error> {
    Ok(SAPI::new(&sapi_address, 60, log.clone()))
}

fn parse_vnodes(msg: &FastMessage) -> Result<(), Error> {
    let v: Vec<Value> = msg.data.d.as_array().unwrap().to_vec();
    println!("v: {:#?}", v);
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
    let boray_config = config::read_file(BORAY_CONFIG_FILE_PATH);
    let db_url = format!("postgres://{}:{}@{}:{}/{}",
                         boray_config.database.user,
                         boray_config.database.user,
                         boray_config.database.host,
                         boray_config.database.port,
                         boray_config.database.database
                         );
    let conn = establish_connection(db_url);
    let template = Template::new(SCHEMA_STR);
    let mut args = HashMap::new();
    args.insert("vnode", vnode);
    let exec_str = template.render(&args);
    println!("{}", exec_str);
    match conn.batch_execute(&exec_str) {
        Ok(_) => Ok(()),
        Err(e) => Err(std::io::Error::new(ErrorKind::Other, e))
    }
}

fn establish_connection(database_url: String) -> PgConnection {

    PgConnection::establish(&database_url)
        .expect(&format!("Error connecting to {}", database_url))
}

pub fn parse_opts<'a, 'b>(app: String) -> ArgMatches<'a> {
   App::new(app)
        .version(crate_version!())
        .about("Tool to manage postgres schemas for boray")
        .arg(Arg::with_name("eb_address")
            .help("electric-boray address")
            .long("eb_address")
            .takes_value(true),
        )
        .arg(Arg::with_name("eb_port")
            .help("electric-boray port")
            .long("eb_port")
            .takes_value(true),
        )
         .arg(Arg::with_name("sapi_address")
            .help("sapi address")
            .long("sapi_address")
            .takes_value(true),
        )
         .arg(Arg::with_name("sapi_port")
            .help("sapi port")
            .long("sapi_port")
            .takes_value(true),
        )
        .arg(
            Arg::with_name("fast_args")
                .help("JSON-encoded arguments for RPC method call")
                .long("args")
                .takes_value(true)
                .required(true),
        )
        .get_matches()
}

pub fn response_handler(msg: &FastMessage) -> Result<(), Error> {
    match msg.data.m.name.as_str() {
        "getvnodes" => {
            println!("msg: {:#?}", msg);
            parse_vnodes(msg)?;
        }
        _ => println!("Received unrecognized {} response", msg.data.m.name),
    }

    Ok(())
}

pub fn run(
    eb_address: String,
    eb_port: u32,
    args: Value,
    _sapi: SAPI) -> Result<(), Error>
{
    let eb_endpoint = [eb_address, String::from(":"), eb_port.to_string()]
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
        println!("shard: {:#?}", json!([s]));
        let vnode_method = String::from("getvnodes");
        fast_client::send(vnode_method,
                          json!([s]),
                          &mut msg_id,
                          &mut stream).and_then(
            |_| fast_client::receive(&mut stream, response_handler),
        )?;
    }
    Ok(())
}

pub fn main() -> Result<(), Error> {
    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let log = Logger::root(
        Mutex::new(slog_term::FullFormat::new(plain).build()).fuse(),
                   o!("build-id" => "0.1.0"),
    );

    let options = parse_opts(APP.to_string());
    let sapi_address = options.value_of("sapi_address").unwrap_or(DEFAULT_SAPI_ADDRESS).to_string();
    let _sapi_port = value_t!(options, "sapi_address", u32).unwrap_or(DEFAULT_SAPI_PORT);
    let sapi = init_sapi_client(sapi_address, log.clone())?;
    let args = value_t!(options, "fast_args", Value).unwrap_or_else(|e| e.exit());
    println!("args: {:#?}", args);
    let eb_address = options.value_of("eb_address").unwrap_or(DEFAULT_EB_ADDRESS).to_string();
    let eb_port = value_t!(options, "eb_port", u32).unwrap_or(DEFAULT_EB_PORT);

    run(eb_address, eb_port, args, sapi)?;
    Ok(())
    // populate service name per shard
}