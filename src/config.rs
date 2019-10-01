// Copyright 2019 Joyent, Inc.

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

use clap::{value_t, ArgMatches};
use num_cpus;
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
    Trace,
}

impl FromStr for LogLevel {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "critical" => Ok(LogLevel::Critical),
            "error" => Ok(LogLevel::Error),
            "warning" => Ok(LogLevel::Warning),
            "info" => Ok(LogLevel::Info),
            "debug" => Ok(LogLevel::Debug),
            "trace" => Ok(LogLevel::Trace),
            _ => Err("invalid log level"),
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
            LogLevel::Error => "error".into(),
            LogLevel::Warning => "warning".into(),
            LogLevel::Info => "info".into(),
            LogLevel::Debug => "debug".into(),
            LogLevel::Trace => "trace".into(),
        }
    }
}

impl From<LogLevel> for slog::Level {
    fn from(lvl: LogLevel) -> Self {
        match lvl {
            LogLevel::Critical => Level::Critical,
            LogLevel::Error => Level::Error,
            LogLevel::Warning => Level::Warning,
            LogLevel::Info => Level::Info,
            LogLevel::Debug => Level::Debug,
            LogLevel::Trace => Level::Trace,
        }
    }
}

#[derive(Clone, Default, Deserialize)]
pub struct Config {
    /// The logging configuration entries
    pub log: ConfigLog,
    /// The configuration entries controlling the boray server behavior
    pub server: ConfigServer,
    /// The configuraiton entries controlling the boray metrics server
    pub metrics: ConfigMetrics,
    /// The database connection configuration entries
    pub database: ConfigDatabase,
    /// The database connection pool configuration entries
    pub cueball: ConfigCueball,
    /// The configuration entries controlling the behavior of the tokio runtime
    /// used by boray.
    pub tokio: ConfigTokio,
}

#[derive(Clone, Deserialize)]
pub struct ConfigLog {
    /// The logging level for boray to use.
    pub level: LogLevel,
}

impl Default for ConfigLog {
    fn default() -> Self {
        ConfigLog {
            level: LogLevel::Info,
        }
    }
}

#[derive(Clone, Deserialize)]
pub struct ConfigServer {
    /// The IP address boray should use to listen for incoming connections.
    pub host: String,
    /// The port number boray should listen on for incoming connections.
    pub port: u16,
}

impl Default for ConfigServer {
    fn default() -> Self {
        ConfigServer {
            host: "127.0.0.1".into(),
            port: 2030,
        }
    }
}

#[derive(Clone, Deserialize)]
pub struct ConfigMetrics {
    /// The IP address boray should use to listen for metrics requests
    pub host: String,
    /// The port number boray should listen on for incoming metrics request connections.
    pub port: u16,
}

impl Default for ConfigMetrics {
    fn default() -> Self {
        ConfigMetrics {
            host: "127.0.0.1".into(),
            port: 3020,
        }
    }
}

#[derive(Clone, Deserialize)]
pub struct ConfigDatabase {
    /// The database username
    pub user: String,
    /// The database IP address
    pub host: String,
    /// The database port number
    pub port: u16,
    /// The name of the database to issue requests against
    pub database: String,
    /// The name of the application to use when connecting to the database
    pub application_name: String,
    /// The TLS connection mode
    pub tls_mode: TlsConnectMode,
    /// The optional path to a TLS certificate file
    pub certificate: Option<PathBuf>,
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
            certificate: None,
        }
    }
}

#[derive(Clone, Deserialize)]
pub struct ConfigCueball {
    /// The maximum number of connections in the postgres connection pool. The default value is 64 connections.
    pub max_connections: u32,
    /// The time in milliseconds that a request to claim a connection from the cueball connection pool should wait before returning an error. The default is 500 ms.
    pub claim_timeout: Option<u64>,
    /// The time in milliseconds to wait prior to rebalancing the connection
    /// pool when a notification is received from the resolver regarding a
    /// change in the service topology. For the case of boray using the postgres
    /// primary resolver this delay should not be very high. The default value
    /// is 20 ms.
    pub rebalancer_action_delay: Option<u64>,
}

impl Default for ConfigCueball {
    fn default() -> Self {
        ConfigCueball {
            max_connections: 64,
            claim_timeout: Some(500),
            rebalancer_action_delay: Some(20),
        }
    }
}

#[derive(Clone, Deserialize)]
pub struct ConfigTokio {
    /// The maximum number of worker threads for the Tokio Runtime's thread
    /// pool. This must be a number between 1 and 32,768 though it is advised to
    /// keep this value on the smaller side. The default value is the number of
    /// logical cores available to the system.
    pub core_threads: Option<usize>,
    /// The maximum number of concurrent blocking sections in the Runtime's
    /// thread pool. When the maximum concurrent blocking calls is reached, any
    /// further calls to blocking will return `NotReady` and the task is
    /// notified once previously in-flight calls to blocking return. This must
    /// be a number between 1 and 32,768 though it is advised to keep this value
    /// on the smaller side. The default value is 200.
    pub blocking_threads: usize,
    /// The worker thread keep alive duration for threads in the Tokio Runtime's
    /// thread pool. If set, a worker thread will wait for up to the specified
    /// duration (in seconds) for work, at which point the thread will
    /// shutdown. When work becomes available, a new thread will eventually be
    /// spawned to replace the one that shut down. When the value is `None`
    /// (*i.e.* It is omitted from the configuration file), the thread will wait
    /// for work forever. The default value is `None`.
    pub thread_keep_alive: Option<u64>,
    /// The stack size (in bytes) for worker threads. The default is 2 MiB.
    pub thread_stack_size: usize,
    /// The name prefix of threads spawned by the Tokio Runtime's thread
    /// pool. The default is `boray-woker-`.
    pub thread_name_prefix: String,
}

impl Default for ConfigTokio {
    fn default() -> Self {
        ConfigTokio {
            core_threads: Some(num_cpus::get().max(1)),
            blocking_threads: 200,
            thread_keep_alive: None,
            thread_stack_size: 2 * 1024 * 1024,
            thread_name_prefix: "boray-worker-".into(),
        }
    }
}

pub fn read_file<F: AsRef<OsStr> + ?Sized>(f: &F) -> Config {
    let s = match fs::read(Path::new(&f)) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to read config file: {}", e);
            std::process::exit(1);
        }
    };

    let mut config: Config = toml::from_slice(&s).unwrap_or_else(|e| {
        eprintln!("Failed to parse config file: {}", e);
        std::process::exit(1);
    });

    if config.tokio.core_threads.is_none() {
        config.tokio.core_threads = Some(num_cpus::get().max(1))
    }

    config
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
        IOError(io::Error),
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
                TlsError::NoCertificate => write!(fmt, "no TLS certificate file given"),
                TlsError::CertError(ref e) => e.fmt(fmt),
                TlsError::IOError(ref e) => e.fmt(fmt),
            }
        }
    }

    pub(crate) fn tls_config(
        mode: TlsConnectMode,
        o_p: Option<PathBuf>,
    ) -> Result<TlsConfig, TlsError> {
        let cert_result = maybe_read_certificate(o_p);
        let cert_ok = cert_result.is_ok();

        match mode {
            TlsConnectMode::Disable => Ok(TlsConfig::disable()),
            TlsConnectMode::Allow => {
                let cert_option = cert_result.ok();
                Ok(TlsConfig::allow(cert_option))
            }
            TlsConnectMode::Prefer => {
                let cert_option = cert_result.ok();
                Ok(TlsConfig::prefer(cert_option))
            }
            TlsConnectMode::Require if cert_ok => {
                // Unwrapping the result since we've verifed the Result is Ok in
                // the guard
                Ok(TlsConfig::require(cert_result.unwrap()))
            }
            TlsConnectMode::VerifyCa if cert_ok => {
                // Unwrapping the result since we've verifed the Result is Ok in
                // the guard
                Ok(TlsConfig::verify_ca(cert_result.unwrap()))
            }
            TlsConnectMode::VerifyFull if cert_ok => {
                // Unwrapping the result since we've verifed the Result is Ok in
                // the guard
                Ok(TlsConfig::verify_full(cert_result.unwrap()))
            }
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
    fn maybe_read_certificate(o_p: Option<PathBuf>) -> Result<Certificate, TlsError> {
        let mut buf = vec![];

        o_p.ok_or(TlsError::NoCertificate)
            .and_then(|p| File::open(p).map_err(Into::into))
            .and_then(|mut f| f.read_to_end(&mut buf).map_err(Into::into))
            .and_then(|_| read_certificate(&buf).map_err(Into::into))
    }

    fn read_certificate(buf: &[u8]) -> Result<Certificate, CertificateError> {
        Certificate::from_der(buf)?;
        Certificate::from_pem(buf)
    }
}
