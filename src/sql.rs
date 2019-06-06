/*
 * Copyright 2019 Joyent, Inc.
 */

use std::time::Instant;
use std::vec::Vec;

use postgres::{Transaction, Client, ToStatement};
use postgres::types::ToSql;
use tokio_postgres::Error as PGError;
use tokio_postgres::Row as PGRow;

use crate::metrics;
use crate::util;

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
    ObjectDeleteMove
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
            Method::ObjectDeleteMove => "ObjectDeleteMove"
        }
    }
}

// conn.execute wrapper that posts metrics
pub fn execute<T>(method: Method,
       conn: &mut Client,
       sql: &T,
       items: &[&dyn ToSql])
       -> Result<u64, PGError>
       where T: ?Sized + ToStatement,
{
    sql_with_metrics(method, || conn.execute(sql, items))
}

// txn.execute wrapper that posts metrics
pub fn txn_execute<T>(method: Method,
       txn: &mut Transaction,
       sql: &T,
       items: &[&dyn ToSql])
       -> Result<u64, PGError>
       where T: ?Sized + ToStatement,
{
    sql_with_metrics(method, || txn.execute(sql, items))
}

// conn.query wrapper that posts metrics
pub fn query<T>(method: Method,
       conn: &mut Client,
       sql: &T,
       items: &[&dyn ToSql])
       -> Result<Vec<PGRow>, PGError>
       where T: ?Sized + ToStatement,
{
    sql_with_metrics(method, || conn.query(sql, items))
}

// txn.query wrapper that posts metrics
pub fn txn_query<T>(method: Method,
       txn: &mut Transaction,
       sql: &T,
       items: &[&dyn ToSql])
       -> Result<Vec<PGRow>, PGError>
       where T: ?Sized + ToStatement,
{
    sql_with_metrics(method, || txn.query(sql, items))
}

fn sql_with_metrics<F, T>(method: Method,
    f: F)
    -> Result<T, PGError>
    where F: FnOnce() -> Result<T, PGError>,
{
    let now = Instant::now();

    let res = f();

    post_timer_metrics(method, now, res.is_ok());

    res
}

fn post_timer_metrics(method: Method, now: Instant, success: bool)
{
    // Generate metrics for the request
    let duration = now.elapsed();
    let t = util::duration_to_seconds(duration);

    let success = match success {
        true => "true",
        false => "false"
    };

    metrics::POSTGRES_REQUESTS.with_label_values(&[&method.as_str(), success]).observe(t);
}
