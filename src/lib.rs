// Copyright 2019 Joyent, Inc.

pub mod bucket;
pub mod error;
pub mod metrics;
pub mod object;
pub mod opts;
pub mod sql;

pub mod util {
    use std::collections::HashMap;
    use std::io::Error as IOError;
    use std::io::ErrorKind;
    use std::time::{Duration, Instant};

    use postgres::error::Error as PGError;
    use postgres::row::Row;
    use serde_json::{json, Value};
    use slog::{error, warn, Logger};

    use cueball::backend::Backend;
    use cueball::connection_pool::ConnectionPool;
    use cueball::error::Error as CueballError;
    use cueball_postgres_connection::PostgresConnection;
    use cueball_static_resolver::StaticIpResolver;
    use rust_fast::protocol::{FastMessage, FastMessageData};

    use crate::bucket;
    use crate::metrics;
    use crate::object;

    pub type Rows = Vec<Row>;
    pub type PostgresResult<T> = Result<T, PGError>;
    pub type Hstore = HashMap<String, Option<String>>;
    pub type Timestamptz = chrono::DateTime<chrono::Utc>;

    pub(crate) enum HandlerError {
        Cueball(CueballError),
        IO(IOError),
    }

    pub(crate) enum HandlerResponse {
        Message(FastMessage),
        Messages(Vec<FastMessage>),
    }

    impl From<FastMessage> for HandlerResponse {
        fn from(fm: FastMessage) -> Self {
            HandlerResponse::Message(fm)
        }
    }

    impl From<Vec<FastMessage>> for HandlerResponse {
        fn from(fms: Vec<FastMessage>) -> Self {
            HandlerResponse::Messages(fms)
        }
    }

    pub fn msg_handler(
        msg: &FastMessage,
        pool: &ConnectionPool<
            PostgresConnection,
            StaticIpResolver,
            impl FnMut(&Backend) -> PostgresConnection + Send + 'static,
        >,
        log: &Logger,
    ) -> Result<Vec<FastMessage>, IOError> {
        let now = Instant::now();
        let mut response: Vec<FastMessage> = vec![];

        metrics::INCOMING_REQUEST_COUNTER.inc();

        let mut connection_acquired = true;
        let method = msg.data.m.name.as_str();

        pool.claim()
            .map_err(HandlerError::Cueball)
            .and_then(|mut conn| {
                // Dispatch the request to the proper handler
                match method {
                    "getobject" => object::get::handler(msg.id, &msg.data.d, &mut conn, &log),
                    "createobject" => object::create::handler(msg.id, &msg.data.d, &mut conn, &log),
                    "updateobject" => object::update::handler(msg.id, &msg.data.d, &mut conn, &log),
                    "deleteobject" => object::delete::handler(msg.id, &msg.data.d, &mut conn, &log),
                    "listobjects" => object::list::handler(msg.id, &msg.data.d, &mut conn, &log),
                    "getbucket" => bucket::get::handler(msg.id, &msg.data.d, &mut conn, &log),
                    "createbucket" => bucket::create::handler(msg.id, &msg.data.d, &mut conn, &log),
                    "deletebucket" => bucket::delete::handler(msg.id, &msg.data.d, &mut conn, &log),
                    "listbuckets" => bucket::list::handler(msg.id, &msg.data.d, &mut conn, &log),
                    _ => {
                        let err_msg = format!("Unsupported functon: {}", method);
                        Err(HandlerError::IO(other_error(&err_msg)))
                    }
                }
            })
            .or_else(|err| {
                // An error occurred while attempting to acquire a connection
                // from the connection pool.
                connection_acquired = false;
                match err {
                    HandlerError::Cueball(CueballError::ClaimFailure) => {
                        // If the error was due to a claim timeout return an
                        // application level error indicating the service is
                        // overloade as a normal Fast message so the calling
                        // application can take appropriate action.
                        warn!(log, "{}", CueballError::ClaimFailure);
                        let value = array_wrap(json!({
                            "name": "OverloadedError",
                            "message": CueballError::ClaimFailure.to_string()
                        }));

                        let msg_data = FastMessageData::new(method.into(), value);
                        let msg: HandlerResponse = FastMessage::data(msg.id, msg_data).into();
                        Ok(msg)
                    }
                    HandlerError::Cueball(err) => {
                        // Any other connection pool errors are unexpected in
                        // this context so log loudly and return an error.
                        error!(log, "{}", err);
                        Err(HandlerError::Cueball(err))
                    }
                    err => Err(err),
                }
            })
            .and_then(|res| {
                // Add application level response to the `response` vector
                match res {
                    HandlerResponse::Message(msg) => response.push(msg),
                    HandlerResponse::Messages(mut msgs) => response.append(&mut msgs),
                }
                Ok(response)
            })
            .and_then(|res| {
                // If we are here the request was successful or we failed to get
                // a connection prior to the connection claim timeout.

                // Generate metrics for the request
                let duration = now.elapsed();
                let t = duration_to_seconds(duration);

                let success = if connection_acquired { "true" } else { "false" };

                metrics::FAST_REQUESTS
                    .with_label_values(&[&method, success])
                    .observe(t);

                Ok(res)
            })
            .or_else(|err| {
                // If we are here, then the request failed

                // Generate metrics for the request
                let duration = now.elapsed();
                let t = duration_to_seconds(duration);

                metrics::FAST_REQUESTS
                    .with_label_values(&[&method, "false"])
                    .observe(t);

                let ret_err = match err {
                    HandlerError::Cueball(cueball_err) => {
                        other_error(cueball_err.to_string().as_str())
                    }
                    HandlerError::IO(io_err) => io_err,
                };

                Err(ret_err)
            })
    }

    pub(crate) fn duration_to_seconds(d: Duration) -> f64 {
        let nanos = f64::from(d.subsec_nanos()) / 1e9;
        d.as_secs() as f64 + nanos
    }

    pub(crate) fn array_wrap(v: Value) -> Value {
        Value::Array(vec![v])
    }

    pub(crate) fn other_error(msg: &str) -> IOError {
        IOError::new(ErrorKind::Other, String::from(msg))
    }
}
