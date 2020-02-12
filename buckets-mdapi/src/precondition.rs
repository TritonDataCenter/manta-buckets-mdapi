// Copyright 2020 Joyent, Inc.

use std::error::Error;
use std::fmt;

use postgres::types::ToSql;
use postgres::Transaction;
use serde_json;
use slog::{crit, Logger};
use tokio_postgres;
use uuid::Uuid;

use crate::error;
use crate::metrics;
use crate::sql;
use crate::types;

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
//
// Can the method here be bounded to only the two methods we expect (ObjectGet and BucketGet)?  Or
// should the function itself make that assertion?
pub fn request(
    method: sql::Method,
    mut txn: &mut Transaction,
    items: &[&dyn ToSql],
    vnode: u64,
    headers: &types::Hstore,
    metrics: &metrics::RegisteredMetrics,
    log: &Logger,
) -> Result<(), ConditionalError> {
    if !is_conditional(headers) {
        crit!(log, "request not conditional; returning");
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
    // For now it's accepting a sql::Method, so we'll see how that goes.
    let rows = sql::txn_query(
        method,
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

        let c = check_conditional(headers, row.get("id"), row.get("modified"));

        match c {
            Ok(x) => x,
            Err(e) => {
                return Err(ConditionalError::Conditional(e));
            }
        };
    }

    Ok(())
}

pub fn is_conditional(headers: &types::Hstore) -> bool {
    headers.contains_key("if-match")
        || headers.contains_key("if-none-match")
        || headers.contains_key("if-modified-since")
        || headers.contains_key("if-unmodified-since")
}

// XXX
//
// Not totally sure on the return type for this.  This needs to at least be a string, but an
// actual error type might be better.  Should this return BucketsMdapiError with an appropriate
// message?  Or should it return many errors/messages, one for each of the failures?
//
// Also not totally sure about the response codes here.  I'm not sure it makes sense for mdapi to
// care so much about HTTP response codes and to let the api handle the assignments of errors
// types/names to response codes.
//
// manta-buckets-api also looks to provide a way of doing either match or modified conditionals.
// Do we need to do the same here?
pub fn check_conditional(
    headers: &types::Hstore,
    etag: Uuid,
    last_modified: types::Timestamptz,
) -> Result<(), error::BucketsMdapiError> {
    // XXX
    //
    // Need to figure out properly getting into Some(Some("x"))
    let x = headers.get("if-match").unwrap().clone();
    let if_match = x.unwrap();

    let mut matched: bool = false;

    // XXX
    //
    // - How is this handling whitespace around the comma?
    // - Are we still ignoring weak comparisons?
    for client_etag in if_match.split(",") {
        let client_etag = client_etag.replace("\"", "");
        if client_etag == "*" || etag.to_string() == client_etag {
            matched = true;
            break;
        }
    }

    if matched {
        Ok(())
    } else {
        let msg =
            format!("if-match '{}' didn't match etag '{}'", if_match, etag);
        //crit!(log, "{}", msg);
        Err(error::BucketsMdapiError::with_message(
            error::BucketsMdapiErrorType::PreconditionFailedError,
            msg,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::collections::HashMap;

    #[test]
    fn precon_empty_headers() {
        let h = HashMap::new();
        assert_eq!(is_conditional(&h), false);
    }

    #[test]
    fn precon_not_applicable_headers() {
        let mut h = HashMap::new();
        let _ = h.insert("if-something".into(), Some("test".into()));
        let _ = h.insert("accept".into(), Some("test".into()));
        assert_eq!(is_conditional(&h), false);
    }

    #[test]
    fn precon_mix_headers() {
        let mut h = HashMap::new();
        let _ = h.insert("if-match".into(), Some("test".into()));
        let _ = h.insert("if-modified-since".into(), Some("test".into()));
        let _ = h.insert("if-none-match".into(), Some("test".into()));
        assert_eq!(is_conditional(&h), true);
    }

    #[test]
    fn precon_check_if_match_single() {
        let id = Uuid::new_v4();
        let modified = Utc::now();

        let mut h = HashMap::new();
        let _ = h.insert("if-match".into(), Some(id.to_string()));
        assert!(check_conditional(&h, id, modified).is_ok());
    }

    #[test]
    fn precon_check_if_match_list() {
        let id = Uuid::new_v4();
        let modified = Utc::now();

        let mut h = HashMap::new();
        let _ = h.insert("if-match".into(), Some(format!("thing,\"{}\"", id)));
        assert!(check_conditional(&h, id, modified).is_ok());
    }

    #[test]
    fn precon_check_if_match_list_with_any() {
        let id = Uuid::new_v4();
        let modified = Utc::now();

        let mut h = HashMap::new();
        let _ = h.insert("if-match".into(), Some("\"test\",thing,*".into()));
        assert!(check_conditional(&h, id, modified).is_ok());
    }

    #[test]
    fn precon_check_if_match_any() {
        let id = Uuid::new_v4();
        let modified = Utc::now();

        let mut h = HashMap::new();
        let _ = h.insert("if-match".into(), Some("*".into()));
        assert!(check_conditional(&h, id, modified).is_ok());
    }
}
