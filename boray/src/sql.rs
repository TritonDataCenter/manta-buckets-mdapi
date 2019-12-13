// Copyright 2019 Joyent, Inc.

use std::fmt::Display;
use std::time::Instant;
use std::vec::Vec;

use postgres::types::ToSql;
use postgres::{Client, ToStatement, Transaction};
use serde_json::Value;
use tokio_postgres::Error as PGError;
use tokio_postgres::Row as PGRow;

use slog::{o, trace, Logger};

use crate::metrics;
use crate::util;
use crate::error::{BorayError, BorayErrorType};

#[derive(Clone, Copy)]
pub enum Method {
    BucketCreate,
    BucketGet,
    BucketList,
    BucketDeleteMove,
    BucketDelete,
    ObjectCreate,
    ObjectCreateMove,
    ObjectGet,
    ObjectList,
    ObjectDelete,
    ObjectDeleteMove,
    ObjectUpdate,
}

impl Method {
    fn as_str(&self) -> &str {
        match self {
            Method::BucketCreate => "BucketCreate",
            Method::BucketGet => "BucketGet",
            Method::BucketList => "BucketList",
            Method::BucketDeleteMove => "BucketDeleteMove",
            Method::BucketDelete => "BucketDelete",
            Method::ObjectCreate => "ObjectCreate",
            Method::ObjectCreateMove => "ObjectCreateMove",
            Method::ObjectGet => "ObjectGet",
            Method::ObjectList => "ObjectList",
            Method::ObjectDelete => "ObjectDelete",
            Method::ObjectDeleteMove => "ObjectDeleteMove",
            Method::ObjectUpdate => "ObjectUpdate",
        }
    }
}

// Create a postgres error object
pub fn postgres_error(msg: String) -> Value {
    serde_json::to_value(BorayError::with_message(
        BorayErrorType::PostgresError, msg))
        .expect("failed to encode a PostgresError error")
}

// conn.execute wrapper that posts metrics
pub fn execute<T: Display>(
    method: Method,
    conn: &mut Client,
    sql: &T,
    items: &[&dyn ToSql],
    log: &Logger,
) -> Result<u64, PGError>
where
    T: ?Sized + ToStatement,
{
    let q_log = log.new(o!("op" => "sql::execute"));
    trace!(q_log, "begin";
        "sql" => sql.to_string(),
        "items" => format!("{:?}", items),
    );
    sql_with_metrics(method, &log, || conn.execute(sql, items))
}

// txn.execute wrapper that posts metrics
pub fn txn_execute<T: Display>(
    method: Method,
    txn: &mut Transaction,
    sql: &T,
    items: &[&dyn ToSql],
    log: &Logger,
) -> Result<u64, PGError>
where
    T: ?Sized + ToStatement,
{
    let q_log = log.new(o!("op" => "sql::txn_execute"));
    trace!(q_log, "begin";
        "sql" => sql.to_string(),
        "items" => format!("{:?}", items),
    );
    sql_with_metrics(method, &log, || txn.execute(sql, items))
}

// conn.query wrapper that posts metrics
pub fn query<T: Display>(
    method: Method,
    conn: &mut Client,
    sql: &T,
    items: &[&dyn ToSql],
    log: &Logger,
) -> Result<Vec<PGRow>, PGError>
where
    T: ?Sized + ToStatement,
{
    let q_log = log.new(o!("op" => "sql::query"));
    trace!(q_log, "begin";
        "sql" => sql.to_string(),
        "items" => format!("{:?}", items),
    );
    sql_with_metrics(method, &q_log, || conn.query(sql, items))
}

// txn.query wrapper that posts metrics
pub fn txn_query<T: Display>(
    method: Method,
    txn: &mut Transaction,
    sql: &T,
    items: &[&dyn ToSql],
    log: &Logger,
) -> Result<Vec<PGRow>, PGError>
where
    T: ?Sized + ToStatement,
{
    let q_log = log.new(o!("op" => "sql::txn_query"));
    trace!(q_log, "begin";
        "sql" => sql.to_string(),
        "items" => format!("{:?}", items),
    );
    sql_with_metrics(method, &q_log, || txn.query(sql, items))
}

fn sql_with_metrics<F, T>(method: Method, log: &Logger, f: F) -> Result<T, PGError>
where
    F: FnOnce() -> Result<T, PGError>,
{
    let now = Instant::now();

    let res = f();

    post_timer_metrics(method, &log, now, &res);

    res
}

fn post_timer_metrics<T>(method: Method, log: &Logger, now: Instant, res: &Result<T, PGError>) {
    // Generate metrics for the request
    let duration = now.elapsed();
    let t = util::duration_to_seconds(duration);

    trace!(log, "end";
        "success" => res.is_ok(),
        "duration_ms" => duration.as_millis(),
    );

    let success = if res.is_ok() { "true" } else { "false" };

    metrics::POSTGRES_REQUESTS
        .with_label_values(&[&method.as_str(), success])
        .observe(t);
}
