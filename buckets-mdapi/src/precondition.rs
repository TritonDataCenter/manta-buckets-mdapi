// Copyright 2020 Joyent, Inc.

use std::error::Error;
use std::fmt;

use tokio_postgres;
use postgres::types::ToSql;
use postgres::Transaction;
use slog::{crit, Logger};
use uuid::Uuid;
use serde_json;

use crate::error;
use crate::sql;
use crate::types;
use crate::metrics;

pub fn error(msg: String) -> serde_json::Value {
    serde_json::to_value(error::BucketsMdapiError::with_message(
        error::BucketsMdapiErrorType::PreconditionFailedError,
        msg,
    ))
    .expect("failed to encode a PreconditionFailedError error")
}

// XXX
//
// This is basically from:
//
//     https://blog.burntsushi.net/rust-error-handling/#defining-your-own-error-type
//
// I can't say I understand it all yet.
//
// I think `conditional` (or whatever it's going to be called) is going to need to have a custom
// error type because it could either be a PGError from attempting the initial query, or whatever
// type the actual conditional comparison failure is going to be.
//
// Still not sure on what error to return to the client.  I think as it is this will look like a
// PostgresError, where I think something like PreconditionFailedError is what I want.  This
// error could then define some ways of printing the precondition failure into a string suitable
// for the fast message.
#[derive(Debug)]
pub enum ConditionalError {
    Pg(tokio_postgres::Error),
    Conditional(error::BucketsMdapiError),
}

impl From<tokio_postgres::Error> for ConditionalError {
    fn from(err: tokio_postgres::Error) -> ConditionalError {
        ConditionalError::Pg(err)
    }
}

impl From<error::BucketsMdapiError> for ConditionalError {
    fn from(err: error::BucketsMdapiError) -> ConditionalError {
        ConditionalError::Conditional(err)
    }
}

impl fmt::Display for ConditionalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ConditionalError::Pg(ref err) => write!(f, "{}", err),
            ConditionalError::Conditional(ref err) => write!(f, "{}", err),
        }
    }
}

impl Error for ConditionalError {
    fn description(&self) -> &str {
        match *self {
            ConditionalError::Pg(ref err) => err.description(),
            ConditionalError::Conditional(ref err) => err.description(),
        }
    }

    fn cause(&self) -> Option<&Error> {
        match *self {
            ConditionalError::Pg(ref err) => Some(err),
            ConditionalError::Conditional(ref err) => Some(err),
        }
    }
}

// XXX
//
// This perhaps could return an Option, but I think it's going to have to be Result at least
// because of the get request case.  In that case the actual call is exactly the same as what is
// done in this conditional call, so we might as well just return the object(s) and have the caller
// skip the separate call to the database.
pub fn request(
    mut txn: &mut Transaction,
    items: &[&dyn ToSql],
    vnode: u64,
    headers: &types::Hstore,
    metrics: &metrics::RegisteredMetrics,
    log: &Logger,
) -> Result<(), ConditionalError> {
    // XXX
    //
    // This should be some kind of `.isConditional()` call on some part of the request.  Probably
    // the headers.
    if !headers.contains_key("if-match") {
        crit!(log, "if-match not found; returning");
        return Ok(());
    }

    // XXX
    //
    // Should this be exactly the same as ObjectGet?  I think we only need the etag, so it's a
    // shame to return all the other fields for no reason.
    //
    // Using ObjectGet has the advantage of possibly returning this as-is in the event that the
    // request is a GetObject.  We can just return `rows`.
    //
    // Also this needs to work for buckets.  Maybe it's just as simple as accepting either
    // BucketGet or ObjectGet as a parameter, or maybe matching on the method name of the request.
    // Maybe I'll add an argument that will either be "bucket" or "object" to make this decision.
    let rows = sql::txn_query(
        sql::Method::ObjectGet,
        &mut txn,
        sql::get_sql(vnode).as_str(),
        items,
        metrics,
        log,
    )?;

    // XXX
    //
    // What happens on 0 rows, like a precondition on a non-existent object?
    //
    // What about >1 rows?  Do we need to handle all cases like `response` does?

    crit!(log, "got {} rows from conditional query", rows.len());

    if rows.len() == 1 {
        let row = &rows[0];

        let etag: Uuid = row.get("id");

        // XXX
        //
        // Need to figure out properly getting into Some(Some("x"))
        let x = headers.get("if-match").unwrap().clone();
        let y = x.unwrap();

        // XXX
        //
        // This could be a comma separated list of values.  Or they could be weak comparisons.
        // Or...
        //
        // Should this headers object be a new type with some fancy methods for getting at this
        // information?
        let if_match_id = Uuid::parse_str(y.as_str()).unwrap();

        if etag == if_match_id {
            crit!(log, "precondition success: want:{} / got:{}", if_match_id, etag);
        } else {
            let msg = format!("if-match {} didn't match etag {}", if_match_id, etag);
            crit!(log, "{}", msg);
            //return Err(ConditionalError::Conditional(io::Error::new(io::ErrorKind::Other, msg)));
            return Err(ConditionalError::Conditional(error::BucketsMdapiError::with_message(
                error::BucketsMdapiErrorType::PreconditionFailedError,
                msg,
            )));
        }
    }

    Ok(())

}
