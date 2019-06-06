/*
 * Copyright 2019 Joyent, Inc.
 */

pub mod bucket;
pub mod metrics;
pub mod object;
pub mod opts;
pub mod sql;

pub mod util {
    use std::io::{Error, ErrorKind};
    use std::time::{Duration, Instant};

    use cueball::connection_pool::ConnectionPool;
    use cueball::backend::Backend;
    use cueball_postgres_connection::PostgresConnection;
    use cueball_static_resolver::StaticIpResolver;
    use postgres::error::Error as PGError;
    use postgres::row::Row;
    use rust_fast::protocol::FastMessage;
    use serde_json::Value;
    use slog::Logger;

    use crate::bucket;
    use crate::metrics;
    use crate::object;

    pub type Rows = Vec<Row>;
    pub type PostgresResult<T> = Result<T, PGError>;

    pub fn other_error(msg: &str) -> Error {
        Error::new(ErrorKind::Other, String::from(msg))
    }

    pub fn duration_to_seconds(d: Duration) -> f64 {
        let nanos = f64::from(d.subsec_nanos()) / 1e9;
        d.as_secs() as f64 + nanos
    }

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
            "getobject"    => object::get_handler(msg.id, &args, response, &pool, &log),
            "createobject" => object::create_handler(msg.id, &args, response, &pool, &log),
            "deleteobject" => object::delete_handler(msg.id, &args, response, &pool, &log),
            "listobjects"  => object::list_handler(msg.id, &args, response, &pool, &log),
            "getbucket"    => bucket::get_handler(msg.id, &args, response, &pool, &log),
            "createbucket" => bucket::create_handler(msg.id, &args, response, &pool, &log),
            "deletebucket" => bucket::delete_handler(msg.id, &args, response, &pool, &log),
            "listbuckets"  => bucket::list_handler(msg.id, &args, response, &pool, &log),
            _ => return Err(Error::new(ErrorKind::Other, format!("Unsupported functon: {}", method)))
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
}
