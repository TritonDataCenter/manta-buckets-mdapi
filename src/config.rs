/*
 * Copyright 2019 Joyent, Inc.
 */

/// Data structures and helper functions for boray configuration.
///
/// Default configuration parameters are set in the different `Default` trait
/// implementations in this module.  These can first be overridden by a config
/// file specified with `-c <config>`, and then overridden again by specific
/// command line arguments.

use std::convert::Into;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use clap::{ArgMatches, value_t};
use serde_derive::Deserialize;
use slog::Level;

use cueball_postgres_connection::TlsConnectMode;

/// A type representing the valid logging levels in a boray configuration.
///
/// This is necessary only because of there is not a serde `Derserialize`
/// implementation available for the `slog::Level` type.
#[derive(Clone, Deserialize)]
pub enum LogLevel {
    /// Log critical level only
    #[serde(alias = "critical")]
    Critical,
    /// Log only error level and above
    #[serde(alias = "error")]
    Error,
    /// Log only warning level and above
    #[serde(alias = "warning")]
    Warning,
    /// Log only info level and above
    #[serde(alias = "info")]
    Info,
    /// Log only debug level and above
    #[serde(alias = "debug")]
    Debug,
    /// Log everything
    #[serde(alias = "trace")]
    Trace
}

impl FromStr for LogLevel {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "critical" => Ok(LogLevel::Critical),
            "error"    => Ok(LogLevel::Error),
            "warning"  => Ok(LogLevel::Warning),
            "info"     => Ok(LogLevel::Info),
            "debug"    => Ok(LogLevel::Debug),
            "trace"    => Ok(LogLevel::Trace),
            _          => Err("invalid log level")
        }
    }
}

impl Default for LogLevel {
    fn default() -> Self {
        LogLevel::Debug
    }
}

impl ToString for LogLevel {
    fn to_string(&self) -> String {
        match self {
            LogLevel::Critical => "critical".into(),
            LogLevel::Error    => "error".into(),
            LogLevel::Warning  => "warning".into(),
            LogLevel::Info     => "info".into(),
            LogLevel::Debug    => "debug".into(),
            LogLevel::Trace    => "trace".into()
        }
    }
}

impl From<LogLevel> for slog::Level {
    fn from(lvl: LogLevel) -> Self {
        match lvl {
            LogLevel::Critical => Level::Critical,
            LogLevel::Error    => Level::Error,
            LogLevel::Warning  => Level::Warning,
            LogLevel::Info     => Level::Info,
            LogLevel::Debug    => Level::Debug,
            LogLevel::Trace    => Level::Trace
        }
    }
}

#[derive(Clone, Default, Deserialize)]
pub struct Config {
    pub log: ConfigLog,
    pub server: ConfigServer,
    pub metrics: ConfigMetrics,
    pub database: ConfigDatabase,
    pub cueball: ConfigCueball,
    pub tokio: ConfigTokio
}

#[derive(Clone, Deserialize)]
pub struct ConfigLog {
    pub level: LogLevel
}

impl Default for ConfigLog {
    fn default() -> Self {
        ConfigLog {
            level: LogLevel::Debug
        }
    }
}

#[derive(Clone, Deserialize)]
pub struct ConfigServer {
    pub host: String,
    pub port: u16
}

impl Default for ConfigServer {
    fn default() -> Self {
        ConfigServer {
            host: "127.0.0.1".into(),
            port: 2030
        }
    }
}

#[derive(Clone, Deserialize)]
pub struct ConfigMetrics {
    pub host: String,
    pub port: u16
}

impl Default for ConfigMetrics {
    fn default() -> Self {
        ConfigMetrics {
            host: "127.0.0.1".into(),
            port: 3020
        }
    }
}

#[derive(Clone, Deserialize)]
pub struct ConfigDatabase {
    pub user: String,
    pub host: String,
    pub port: u16,
    pub database: String,
    pub application_name: String,
    pub tls_mode: TlsConnectMode,
    pub certificate: Option<PathBuf>
}

impl Default for ConfigDatabase {
    fn default() -> Self {
        ConfigDatabase {
            user: "postgres".into(),
            host: "127.0.0.1".to_owned(),
            port: 2030,
            database: "boray".to_owned(),
            application_name: "boray".into(),
            tls_mode: TlsConnectMode::Disable,
            certificate: None
        }
    }
}

#[derive(Clone, Deserialize)]
pub struct ConfigCueball {
    pub max_connections: u32,
    pub claim_timeout: Option<u64>,
    pub rebalancer_action_delay: Option<u64>
}

impl Default for ConfigCueball {
    fn default() -> Self {
        ConfigCueball {
            max_connections: 10,
            claim_timeout: Some(500),
            rebalancer_action_delay: Some(100)
        }
    }
}


#[derive(Clone, Deserialize)]
pub struct ConfigTokio {
    pub core_threads: usize,
    pub blocking_threads: usize,
    pub thread_keep_alive: u64,
    pub thread_stack_size: usize,
    pub thread_name_prefix: String
}

impl Default for ConfigTokio {
    fn default() -> Self {
        ConfigTokio {
            core_threads: 4,
            blocking_threads: 200,
            thread_keep_alive: 60,
            thread_stack_size: 2 * 1024 * 1024,
            thread_name_prefix: "boray-worker-".into()
        }
    }
}

pub fn read_file<F: AsRef<OsStr> + ?Sized>(f: &F) -> Config
{
    let s = match fs::read(Path::new(&f)) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to read config file: {}", e);
            std::process::exit(1);
        }
    };

    toml::from_slice(&s)
        .unwrap_or_else(|e| {
            eprintln!("Failed to parse config file: {}", e);
            std::process::exit(1);
        })
}

pub fn read_cli_args(matches: &ArgMatches, config: &mut Config) {
    value_t!(matches, "level", LogLevel)
        .map(|l| config.log.level = l)
        .unwrap_or_else(|_| ());

    if let Some(a) = matches.value_of("address") {
        config.server.host = a.into();
    }

    value_t!(matches, "port", u16)
        .map(|p| config.server.port = p)
        .unwrap_or_else(|_| ());

    if let Some(h) = matches.value_of("pg ip") {
        config.database.host = h.into();
    }

    value_t!(matches, "pg port", u16)
        .map(|p| config.database.port = p)
        .unwrap_or_else(|_| ());

    if let Some(db) = matches.value_of("pg database") {
        config.database.database = db.into();
    }

    if let Some(h) = matches.value_of("metrics-address") {
        config.metrics.host = h.into();
    }

    value_t!(matches, "metrics-port", u16)
        .map(|p| config.metrics.port = p)
        .unwrap_or_else(|_| ());
}

pub mod tls {
    use super::*;

    use std::fmt;
    use std::fs::File;
    use std::io;
    use std::io::Read;

    use cueball_postgres_connection::{Certificate, CertificateError, TlsConfig};

    #[derive(Debug)]
    pub(crate) enum TlsError {
        NoCertificate,
        CertError(CertificateError),
        IOError(io::Error)
    }

    impl From<io::Error> for TlsError {
        fn from(error: io::Error) -> Self {
            TlsError::IOError(error)
        }
    }

    impl From<CertificateError> for TlsError {
        fn from(error: CertificateError) -> Self {
            TlsError::CertError(error)
        }
    }

    impl fmt::Display for TlsError {
        fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            match self {
                TlsError::NoCertificate => {
                    write!(fmt, "no TLS certificate file given")
                },
                TlsError::CertError(ref e) => e.fmt(fmt),
                TlsError::IOError(ref e) => e.fmt(fmt)
            }
        }
    }

    pub(crate) fn tls_config(mode: TlsConnectMode, o_p: Option<PathBuf>) ->
        Result<TlsConfig, TlsError>
    {
        let cert_result = maybe_read_certificate(o_p);
        let cert_ok = cert_result.is_ok();

        match mode {
            TlsConnectMode::Disable => Ok(TlsConfig::disable()),
            TlsConnectMode::Allow => {
                let cert_option = cert_result.ok();
                Ok(TlsConfig::allow(cert_option))
            },
            TlsConnectMode::Prefer => {
                let cert_option = cert_result.ok();
                Ok(TlsConfig::prefer(cert_option))
            },
            TlsConnectMode::Require if cert_ok => {
                // Unwrapping the result since we've verifed the Result is Ok in
                // the guard
                Ok(TlsConfig::require(cert_result.unwrap()))
            },
            TlsConnectMode::VerifyCa if cert_ok => {
                // Unwrapping the result since we've verifed the Result is Ok in
                // the guard
                Ok(TlsConfig::verify_ca(cert_result.unwrap()))
            },
            TlsConnectMode::VerifyFull if cert_ok  => {
                // Unwrapping the result since we've verifed the Result is Ok in
                // the guard
                Ok(TlsConfig::verify_full(cert_result.unwrap()))
            },
            _ => {
                // If we arrive here we know that `cert_result` is an error of the
                // type to be returned, but the compiler does not know this and
                // so we use the `and function here to make the compiler happy
                // with the `Ok` side of the `Result` type.
                cert_result.and(Ok(TlsConfig::disable()))
            }
        }
    }

    /// If a certificate file path is present then open the file, read the
    /// bytes into a buffer, and attempt to interpret the bytes as a
    /// Certificate while handling errors along the way.
    fn maybe_read_certificate(o_p: Option<PathBuf>) ->
        Result<Certificate, TlsError>
    {
        let mut buf = vec![];

        o_p.ok_or(TlsError::NoCertificate)
            .and_then(|p| {
                File::open(p).map_err(Into::into)
            })
            .and_then(|mut f| {
                f.read_to_end(&mut buf).map_err(Into::into)
            })
            .and_then(|_| {
                read_certificate(&buf).map_err(Into::into)
            })
    }

    fn read_certificate(buf: &[u8]) ->
        Result<Certificate, CertificateError>
    {
        Certificate::from_der(buf)?;
        Certificate::from_pem(buf)
    }
}
