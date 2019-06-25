/*
 * Copyright 2019 Joyent, Inc.
 */

pub mod bucket;
pub mod error;
pub mod metrics;
pub mod object;
pub mod opts;
pub mod sql;

pub mod util {
    use std::collections::HashMap;
    use std::io::{Error, ErrorKind};
    use std::time::{Duration, Instant};

    use postgres::error::Error as PGError;
    use postgres::row::Row;
    use serde_json::Value;
    use slog::Logger;

    use cueball::connection_pool::ConnectionPool;
    use cueball::backend::Backend;
    use cueball_postgres_connection::PostgresConnection;
    use cueball_static_resolver::StaticIpResolver;
    use rust_fast::protocol::FastMessage;

    use crate::bucket;
    use crate::metrics;
    use crate::object;

    pub type Rows = Vec<Row>;
    pub type PostgresResult<T> = Result<T, PGError>;
    pub type Hstore = HashMap<String, Option<String>>;
    pub type Timestamptz = chrono::DateTime<chrono::Utc>;

    pub fn msg_handler(msg: &FastMessage,
                       pool: &ConnectionPool<PostgresConnection, StaticIpResolver, impl FnMut(&Backend) -> PostgresConnection + Send + 'static>,
                       log: &Logger) -> Result<Vec<FastMessage>, Error>
    {
        let now = Instant::now();
        let response: Vec<FastMessage> = vec![];

        metrics::INCOMING_REQUEST_COUNTER.inc();

        let args = match msg.data.d {
            Value::Array(ref args) => args,
            _ => {
                return Err(other_error("Expected JSON array"));
            }
        };

        let method = msg.data.m.name.as_str();
        let ret = match method {
            "getobject"    =>
                object::get::handler(msg.id, &args, response, &pool, &log),
            "createobject" =>
                object::create::handler(msg.id, &args, response, &pool, &log),
            "updateobject" =>
                object::update::handler(msg.id, &args, response, &pool, &log),
            "deleteobject" =>
                object::delete::handler(msg.id, &args, response, &pool, &log),
            "listobjects"  =>
                object::list::handler(msg.id, &args, response, &pool, &log),
            "getbucket"    =>
                bucket::get::handler(msg.id, &args, response, &pool, &log),
            "createbucket" =>
                bucket::create::handler(msg.id, &args, response, &pool, &log),
            "deletebucket" =>
                bucket::delete::handler(msg.id, &args, response, &pool, &log),
            "listbuckets"  =>
                bucket::list::handler(msg.id, &args, response, &pool, &log),
            _ => {
                let err_msg = format!("Unsupported functon: {}", method);
                return Err(Error::new(ErrorKind::Other, err_msg))
            }
        };

        // If we are here, then the method name was valid, and the request may or
        // may not have been successful.

        // Generate metrics for the request
        let duration = now.elapsed();
        let t = duration_to_seconds(duration);

        let success = match ret.is_ok() {
            true => "true",
            false => "false"
        };

        metrics::FAST_REQUESTS.with_label_values(&[&method, success]).observe(t);

        ret
    }

    pub(crate) fn duration_to_seconds(d: Duration) -> f64 {
        let nanos = f64::from(d.subsec_nanos()) / 1e9;
        d.as_secs() as f64 + nanos
    }

    pub(crate) fn array_wrap(v: Value) -> Value {
        Value::Array(vec![v])
    }

    pub(crate) fn other_error(msg: &str) -> Error {
        Error::new(ErrorKind::Other, String::from(msg))
    }
}
