// Copyright 2020 Joyent, Inc.

use std::fmt::Display;
use std::marker::Sync;
use std::time::Instant;
use std::vec::Vec;

use postgres::types::ToSql;
use postgres::{Client, ToStatement, Transaction};
use tokio_postgres::Error as PGError;
use tokio_postgres::Row as PGRow;

use slog::{o, trace, Logger};

use crate::metrics;
use crate::util;

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
    GarbageGet,
    GarbageDelete,
    GarbageRecordDelete,
    GarbageBatchIdGet,
    GarbageBatchIdUpdate,
    GarbageRefresh,
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
            Method::GarbageGet => "GarbageGet",
            Method::GarbageDelete => "GarbageDelete",
            Method::GarbageRecordDelete => "GarbageRecordDelete",
            Method::GarbageBatchIdGet => "GarbageBatchIdGet",
            Method::GarbageBatchIdUpdate => "GarbageBatchIdUpdate",
            Method::GarbageRefresh => "GarbageRefresh",
        }
    }
}

// conn.execute wrapper that posts metrics
pub fn execute<T: Display>(
    method: Method,
    conn: &mut Client,
    sql: &T,
    items: &[&(dyn ToSql + Sync)],
    metrics: &metrics::RegisteredMetrics,
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
    sql_with_metrics(method, metrics, &q_log, || conn.execute(sql, items))
}

// txn.execute wrapper that posts metrics
pub fn txn_execute<T: Display>(
    method: Method,
    txn: &mut Transaction,
    sql: &T,
    items: &[&(dyn ToSql + Sync)],
    metrics: &metrics::RegisteredMetrics,
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
    sql_with_metrics(method, metrics, &q_log, || txn.execute(sql, items))
}

// conn.query wrapper that posts metrics
pub fn query<T: Display>(
    method: Method,
    conn: &mut Client,
    sql: &T,
    items: &[&(dyn ToSql + Sync)],
    metrics: &metrics::RegisteredMetrics,
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
    sql_with_metrics(method, metrics, &q_log, || conn.query(sql, items))
}

// txn.query wrapper that posts metrics
pub fn txn_query<T: Display>(
    method: Method,
    txn: &mut Transaction,
    sql: &T,
    items: &[&(dyn ToSql + Sync)],
    metrics: &metrics::RegisteredMetrics,
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
    sql_with_metrics(method, metrics, &q_log, || txn.query(sql, items))
}

fn sql_with_metrics<F, T>(
    method: Method,
    metrics: &metrics::RegisteredMetrics,
    log: &Logger,
    f: F,
) -> Result<T, PGError>
where
    F: FnOnce() -> Result<T, PGError>,
{
    let now = Instant::now();

    let res = f();

    post_timer_metrics(method, metrics, log, now, &res);

    res
}

fn post_timer_metrics<T>(
    method: Method,
    metrics: &metrics::RegisteredMetrics,
    log: &Logger,
    now: Instant,
    res: &Result<T, PGError>,
) {
    // Generate metrics for the request
    let duration = now.elapsed();
    let t = util::duration_to_seconds(duration);

    trace!(log, "end";
        "success" => res.is_ok(),
        "duration_ms" => duration.as_millis(),
    );

    let success = if res.is_ok() { "true" } else { "false" };

    metrics
        .postgres_requests
        .with_label_values(&[&method.as_str(), success])
        .observe(t);
}
