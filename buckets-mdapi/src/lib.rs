// Copyright 2020 Joyent, Inc.

#![allow(clippy::module_name_repetitions)]

pub mod bucket;
pub mod conditional;
pub mod error;
pub mod gc;
pub mod metrics;
pub mod object;
pub mod opts;
pub mod sql;

pub mod util {
    use std::collections::HashMap;
    use std::io::Error as IOError;
    use std::io::ErrorKind;
    use std::thread;
    use std::time::{Duration, Instant};

    use serde_json::Error as SerdeError;
    use serde_json::Value;
    use slog::{debug, error, o, warn, Logger};

    use crossbeam_channel::{bounded, Receiver, Sender};
    use cueball::backend::Backend;
    use cueball::connection_pool::{ConnectionPool, PoolConnection};
    use cueball::error::Error as CueballError;
    use cueball::resolver::Resolver;
    use cueball_postgres_connection::PostgresConnection;
    use fast_rpc::protocol::{FastMessage, FastMessageData};
    use rocksdb::Error as RocksdbError;

    use crate::bucket;
    use crate::error::BucketsMdapiError;
    // use crate::gc;
    use crate::metrics::RegisteredMetrics;
    use crate::object;
    use crate::types::{HandlerError, HandlerResponse, HasRequestId, KVOps};
    use crate::util;

    // Attempt to claim a connection from the cueball connection pool and track
    // the time spent waiting
    fn claim_pool_connection(
        pool: &ConnectionPool<
            PostgresConnection,
            impl Resolver,
            impl FnMut(&Backend) -> PostgresConnection + Send + 'static,
        >,
        metrics: &RegisteredMetrics,
    ) -> Result<
        PoolConnection<
            PostgresConnection,
            impl Resolver,
            impl FnMut(&Backend) -> PostgresConnection + Send + 'static,
        >,
        CueballError,
    > {
        let now = Instant::now();
        let claim_result = pool.claim();

        let duration = now.elapsed();
        let t = util::duration_to_seconds(duration);

        let success = if claim_result.is_ok() {
            "true"
        } else {
            "false"
        };

        metrics
            .connection_claim_times
            .with_label_values(&[success])
            .observe(t);

        claim_result
    }

    pub fn handle_msg(
        msg: &FastMessage,
        send_channel_map: &HashMap<
            u64,
            Sender<(
                KVOps,
                Vec<u8>,
                Option<Vec<u8>>,
                Sender<Result<Option<Vec<u8>>, RocksdbError>>,
            )>,
        >,
        metrics: &RegisteredMetrics,
        log: &Logger,
    ) -> Result<Vec<FastMessage>, IOError> {
        let now = Instant::now();
        let mut response: Vec<FastMessage> = vec![];

        metrics.request_count.inc();

        let mut connection_acquired = true;
        let method = msg.data.m.name.as_str();

        // Dispatch the request
        let request_result = match method {
            "getobject" => handle_request(
                msg.id,
                method,
                object::get::decode_msg(&msg.data.d),
                &send_channel_map,
                &object::get::action,
                metrics,
                log,
            ),
            "createobject" => handle_request(
                msg.id,
                method,
                object::create::decode_msg(&msg.data.d),
                &send_channel_map,
                &object::create::action,
                metrics,
                log,
            ),
            "getbucket" => handle_request(
                msg.id,
                method,
                bucket::get::decode_msg(&msg.data.d),
                &send_channel_map,
                &bucket::get::action,
                metrics,
                log,
            ),
            "createbucket" => handle_request(
                msg.id,
                method,
                bucket::create::decode_msg(&msg.data.d),
                &send_channel_map,
                &bucket::create::action,
                metrics,
                log,
            ),
            _ => {
                let err_msg = format!("Unsupported functon: {}", method);
                Err(HandlerError::IO(other_error(&err_msg)))
            }
        };

        request_result
            .and_then(|res| {
                // Add application level response to the `response` vector
                match res {
                    HandlerResponse::Message(msg) => response.push(msg),
                    HandlerResponse::Messages(mut msgs) => {
                        response.append(&mut msgs)
                    }
                }
                Ok(response)
            })
            .and_then(|res| {
                // If we are here the request was successful or we failed to get
                // a connection prior to the connection claim timeout.

                // Generate metrics for the request
                let duration = now.elapsed();
                let t = duration_to_seconds(duration);

                let success =
                    if connection_acquired { "true" } else { "false" };

                metrics
                    .fast_requests
                    .with_label_values(&[&method, success])
                    .observe(t);

                Ok(res)
            })
            .or_else(|err| {
                // If we are here, then the request failed

                // Generate metrics for the request
                let duration = now.elapsed();
                let t = duration_to_seconds(duration);

                metrics
                    .fast_requests
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

    // Create a LimitConstraintError error object
    pub fn limit_constraint_error(msg: String) -> Value {
        serde_json::to_value(BucketsMdapiError::LimitConstraintError(msg))
            .expect("failed to encode a LimitConstraintError error")
    }

    #[allow(clippy::cast_precision_loss)]
    pub(crate) fn duration_to_seconds(d: Duration) -> f64 {
        let nanos = f64::from(d.subsec_nanos()) / 1e9;
        d.as_secs() as f64 + nanos
    }

    pub(crate) fn array_wrap(v: Value) -> Value {
        Value::Array(vec![v])
    }

    pub(crate) fn unwrap_fast_message<T>(
        operation: &str,
        log: &Logger,
        mut arr: Vec<T>,
    ) -> Result<T, String>
    where
        T: for<'de> serde::Deserialize<'de>,
    {
        // Remove outer JSON array required by Fast
        if arr.is_empty() {
            let err_msg = format!(
                "Failed to parse JSON data as payload for \
                 {} function",
                operation
            );
            warn!(log, "{}", err_msg);
            Err(err_msg)
        } else {
            Ok(arr.remove(0))
        }
    }

    pub(crate) fn other_error(msg: &str) -> IOError {
        IOError::new(ErrorKind::Other, String::from(msg))
    }

    pub fn create_bounded_channel() -> (
        Sender<(
            KVOps,
            Vec<u8>,
            Option<Vec<u8>>,
            Sender<Result<Option<Vec<u8>>, RocksdbError>>,
        )>,
        Receiver<(
            KVOps,
            Vec<u8>,
            Option<Vec<u8>>,
            Sender<Result<Option<Vec<u8>>, RocksdbError>>,
        )>,
    ) {
        bounded(1000)
    }

    /// Handle a valid RPC function request. This function employs higher-ranked
    /// trait bounds in order to specify a constraint that the request type
    /// implement serde's `Deserialize` trait while not having to provide a fixed
    /// lifetime for the constraint. Instead the constraint is specified as being
    /// valid for all lifetimes `'de`. More information about higher-ranked
    /// trait bounds can be found
    /// [here](https://doc.rust-lang.org/nomicon/hrtb.html).
    pub(crate) fn handle_request<X>(
        msg_id: u32,
        method: &str,
        data: Result<Vec<X>, SerdeError>,
        send_channel_map: &HashMap<
            u64,
            Sender<(
                KVOps,
                Vec<u8>,
                Option<Vec<u8>>,
                Sender<Result<Option<Vec<u8>>, RocksdbError>>,
            )>,
        >,
        action: &dyn Fn(
            u32,
            &str,
            &RegisteredMetrics,
            &Logger,
            X,
            &HashMap<
                u64,
                Sender<(
                    KVOps,
                    Vec<u8>,
                    Option<Vec<u8>>,
                    Sender<Result<Option<Vec<u8>>, RocksdbError>>,
                )>,
            >,
        ) -> Result<HandlerResponse, String>,
        metrics: &RegisteredMetrics,
        log: &Logger,
    ) -> Result<HandlerResponse, HandlerError>
    where
        X: for<'de> serde::Deserialize<'de> + HasRequestId,
    {
        let mut log_child = log.new(o!("method" => method.to_string()));

        debug!(log_child, "handling request");

        data.map_err(|e| e.to_string())
            .and_then(|arr| unwrap_fast_message(&method, &log_child, arr))
            .and_then(|payload| {
                // Add the request id to the log output
                let req_id = payload.request_id();
                log_child = log_child.new(o!("req_id" => req_id.to_string()));

                debug!(log_child, "parsed payload");

                // Perform the action indicated by the request
                action(
                    msg_id,
                    &method,
                    metrics,
                    &log_child,
                    payload,
                    send_channel_map,
                )
            })
            .map_err(|e| HandlerError::IO(other_error(&e)))
    }

    pub fn get_thread_name() -> String {
        thread::current()
            .name()
            .unwrap_or_else(|| "unnamed")
            .to_string()
    }
}

pub mod types {
    use std::collections::HashMap;
    use std::io::Error as IOError;

    use postgres::error::Error as PGError;
    use postgres::row::Row;
    use uuid::Uuid;

    use cueball::error::Error as CueballError;
    use fast_rpc::protocol::FastMessage;

    pub type Rows = Vec<Row>;
    pub type RowSlice = [Row];
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

    /// This trait ensures that any implementing type can provide a request UUID
    /// via the `request_id` method. This trait can be used in trait bounds so
    /// that certain assumptions can be made in the code. In particular for this
    /// trait the desired assumption is that a request id field can be added to
    /// all logging output for any valid request type.
    pub trait HasRequestId {
        fn request_id(&self) -> Uuid;
    }

    pub enum KVOps {
        Get,
        Put,
        Delete,
    }
}
