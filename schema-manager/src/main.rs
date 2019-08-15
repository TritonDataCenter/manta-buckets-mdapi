// Copyright 2019 Joyent, Inc.
use clap::{Arg, App};

pub fn main() {
    let matches = App::new("schema-manager")
        .version("0.1.0")
        .author("Jon Anderson <jon.anderson@joyent.com>")
        .about("Tool to manager postgres schemas for boray")
        .arg(Arg::with_name("endpoint")
                 .required(true)
                 .takes_value(true)
                 .index(1)
                 .help("postgres IP address"))
        .get_matches();
    let endpoint = matches.value_of("endpoint").unwrap();
    println!("{}", endpoint);
}