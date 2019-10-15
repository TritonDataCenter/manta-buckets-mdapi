/*
 * Copyright 2019 Joyent, Inc.
 */

use clap::{crate_version, App, Arg, ArgMatches};

pub fn parse<'a>(app: &str) -> ArgMatches<'a> {
    App::new(app)
        .about("Tool to test different hierarchy options offered by PostgreSQL")
        .version(crate_version!())
        .arg(
            Arg::with_name("pg ip")
                .help("Postgres IP address")
                .long("pg-ip")
                .takes_value(true)
                .required(false),
        )
        .arg(
            Arg::with_name("pg port")
                .help("Postgres port")
                .long("pg-port")
                .takes_value(true)
                .required(false),
        )
        .arg(
            Arg::with_name("pg database")
                .help("Postgres database name")
                .long("pg-db")
                .takes_value(true)
                .required(false),
        )
        .arg(
            Arg::with_name("config")
                .help("Configuration file")
                .short("c")
                .long("config")
                .takes_value(true)
                .required(false),
        )
        .arg(
            Arg::with_name("address")
                .help("Listen address")
                .short("a")
                .long("address")
                .takes_value(true)
                .required(false),
        )
        .arg(
            Arg::with_name("port")
                .help("Listen port")
                .short("p")
                .long("port")
                .takes_value(true)
                .required(false),
        )
        .arg(
            Arg::with_name("level")
                .help("Log level")
                .short("l")
                .long("level")
                .takes_value(true)
                .possible_values(&["trace", "debug", "info", "warning", "error", "critical"])
                .required(false),
        )
        .arg(
            Arg::with_name("metrics address")
                .help("Address to listen for metrics queries")
                .long("metrics-address")
                .takes_value(true)
                .required(false),
        )
        .arg(
            Arg::with_name("metrics port")
                .help("Port to listen for metrics queries")
                .long("metrics-port")
                .takes_value(true)
                .required(false),
        )
        .get_matches()
}
