/*
 * Copyright 2019 Joyent, Inc.
 */

use serde_derive::Deserialize;

// Describes the layout of `config.toml`

#[derive(Deserialize)]
pub struct Config {
    pub log: ConfigLog,
    pub server: ConfigServer,
    pub metrics: ConfigMetrics,
    pub database: ConfigDatabase
}

#[derive(Deserialize)]
pub struct ConfigLog {
    pub level: String
}

#[derive(Deserialize)]
pub struct ConfigServer {
    pub host: String,
    pub port: u16
}

#[derive(Deserialize)]
pub struct ConfigMetrics {
    pub host: String,
    pub port: u16
}

#[derive(Deserialize)]
pub struct ConfigDatabase {
    pub host: String,
    pub port: u16,
    pub db: String
}
