/*
 * Copyright 2019 Joyent, Inc.
 */

pub mod bucket;
pub mod metrics;
pub mod object;
pub mod opts;

pub mod util {
    use std::io::{Error, ErrorKind};

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

    pub fn msg_handler(msg: &FastMessage,
                       pool: &ConnectionPool<PostgresConnection, StaticIpResolver, impl FnMut(&Backend) -> PostgresConnection + Send + 'static>,
                       log: &Logger) -> Result<Vec<FastMessage>, Error>
    {
        let response: Vec<FastMessage> = vec![];

        metrics::INCOMING_REQUEST_COUNTER.inc();

        match msg.data.d {
            Value::Array(ref args) => {
                match msg.data.m.name.as_str() {
                    "getobject"    => object::get_handler(msg.id, &args, response, &pool, &log),
                    "putobject"    => object::put_handler(msg.id, &args, response, &pool, &log),
                    "deleteobject" => object::delete_handler(msg.id, &args, response, &pool, &log),
                    "listobjects"  => object::list_handler(msg.id, &args, response, &pool, &log),
                    "getbucket"    => bucket::get_handler(msg.id, &args, response, &pool, &log),
                    "putbucket"    => bucket::put_handler(msg.id, &args, response, &pool, &log),
                    "deletebucket" => bucket::delete_handler(msg.id, &args, response, &pool, &log),
                    "listbuckets"  => bucket::list_handler(msg.id, &args, response, &pool, &log),
                    _ => Err(Error::new(ErrorKind::Other, format!("Unsupported functon: {}", msg.data.m.name)))
                }
            }
            _ => Err(other_error("Expected JSON array"))
        }
    }
}
