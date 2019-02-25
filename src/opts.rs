/*
 * Copyright 2019 Joyent, Inc.
 */

use clap::{App, Arg, ArgMatches};

pub fn parse<'a, 'b>(app: String) -> ArgMatches<'a> {
    App::new(app)
        .about("Tool to test different hierarchy options offered by PostgreSQL")
        .version(crate_version!())
        .arg(Arg::with_name("pg_url")
             .help("Postgres URL")
             .short("u")
             .long("pg-url")
             .takes_value(true)
             .required(false))
        .arg(Arg::with_name("address")
             .help("Listen address")
             .short("a")
             .long("address")
             .takes_value(true)
             .required(false))
        .arg(Arg::with_name("port")
             .help("Listen port")
             .short("p")
             .long("port")
             .takes_value(true)
             .required(false))
        .arg(Arg::with_name("level")
             .help("Log level")
             .short("l")
             .long("level")
             .takes_value(true)
             .required(false),
        )
        .get_matches()
}
