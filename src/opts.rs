extern crate clap;

use clap::{App, Arg, ArgMatches};

pub fn parse<'a, 'b>(app: String) -> ArgMatches<'a> {
    App::new(app)
        .about("Tool to test different hierarchy options offered by PostgreSQL")
        .version(crate_version!())
        .arg(Arg::with_name("url")
             .help("Postgres URL")
             .short("u")
             .long("url")
             .takes_value(true)
             .required(false))
        .get_matches()
}
