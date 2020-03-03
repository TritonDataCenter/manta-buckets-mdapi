// Copyright 2020 Joyent, Inc.

use std::error::Error;
use std::fmt;

use postgres::types::ToSql;
use postgres::Transaction;
use serde_json;
use slog::{crit, trace, Logger};
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

/*
 * XXX
 *
 * This is basically from:
 *
 *     https://blog.burntsushi.net/rust-error-handling/#defining-your-own-error-type
 *
 * I think this is going to need to have a custom error type because as it now it could either be a
 * PGError from attempting the initial query, or whatever type the actual conditional comparison
 * failure is going to be.
 *
 * Really thought I expect it will be more complex than that.  The possible cases are something
 * like:
 *
 * - PreconditionFailedError, from our checks on if-* headers
 * - PgError, from an error talking to the database
 * - <Some other error>, if the query for the object returns >1 rows
 * - ObjectNotFound, if the query for the object returns no rows
 * - BadRequestError, if the date in if-*modified-since is invalid
 *
 * That's quite a few, but I think they're still enumerable at compile time.  Should this just be a
 * Box<Error>?
 */
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

impl From<String> for ConditionalError {
    fn from(msg: String) -> ConditionalError {
        let err = error::BucketsMdapiError::with_message(
            error::BucketsMdapiErrorType::PreconditionFailedError,
            msg,
        );
        ConditionalError::Conditional(err)
    }
}

impl fmt::Display for ConditionalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
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
        Some(self)
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
    conditions: &Option<types::Hstore>,
    metrics: &metrics::RegisteredMetrics,
    log: &Logger,
) -> Result<(), ConditionalError> {
    if !is_conditional(conditions) {
        trace!(log, "request not conditional; returning");
        return Ok(());
    }

    let conditions = conditions.as_ref().unwrap();

    // XXX
    //
    // Should this be exactly the same as ObjectGet?  I think we only need the etag and
    // last_modified, so it's a shame to return all the other fields for no reason.
    //
    // Using ObjectGet has the advantage of possibly returning this as-is in the event that the
    // request is a GetObject.  We can just return `rows`.
    let rows = sql::txn_query(
        sql::Method::ObjectGet,
        &mut txn,
        sql::get_sql(vnode).as_str(),
        items,
        metrics,
        log,
    )?;

    /*
     *
     * What happens on 0 rows, like a precondition on a non-existent object?  Do we return Ok()
     * here and expect/hope the followup query also returns nothing?  That doesn't feel right, so
     * perhaps this should also be returning a "not found" error?
     *
     * What about >1 rows?  Do we need to handle all cases like `response` does?
     */

    crit!(log, "got {} rows from conditional query", rows.len());

    if rows.len() == 1 {
        check_conditional(&conditions, rows[0].get("id"), rows[0].get("modified"))?
    }

    Ok(())
}

pub fn is_conditional(headers: &Option<types::Hstore>) -> bool {
    match headers {
        Some(headers) => {
            headers.contains_key("if-match")
                || headers.contains_key("if-none-match")
                || headers.contains_key("if-modified-since")
                || headers.contains_key("if-unmodified-since")
        },
        None => false
    }
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
//
// I think this needs to only return the first issue in the conditional request it comes across, as
// opposed to a mixed bag of all failures.  I guess this is part of the RFC, but for now I'll do
// this in the same order as joyent/manta-buckets-api, which is:
//
//     if-match > if-unmodified-since > if-none-match > if-modified-since
pub fn check_conditional(
    headers: &types::Hstore,
    etag: Uuid,
    last_modified: types::Timestamptz,
) -> Result<(), String> {
    if let Some(header) = headers.get("if-match") {
        if let Some(if_match) = header {
            let match_client_etags = if_match.split(",").collect();
            if !check_if_match(etag.to_string(), match_client_etags) {
                return Err(format!("if-match '{}' didn't match etag '{}'", if_match, etag));
            }
        }
    }

    if let Some(header) = headers.get("if-unmodified-since") {
        if let Some(client_unmodified_string) = header {
            if let Ok(client_unmodified) =
                client_unmodified_string.parse::<types::Timestamptz>()
            {
                if check_if_modified(last_modified, client_unmodified) {
                    return Err(format!(
                        "object was modified at '{}'; if-unmodified-since '{}'",
                        last_modified.to_rfc3339(), client_unmodified.to_rfc3339(),
                    ));
                }
            } else {
                /*
                 * XXX Moving to String errors means we've lost BadRequestError.
                 */
                return Err(format!(
                    "unable to parse '{}' as a valid date",
                    client_unmodified_string,
                ));
            }
        }
    }

    if let Some(header) = headers.get("if-none-match") {
        if let Some(if_none_match) = header {
            let match_client_etags = if_none_match.split(",").collect();
            if check_if_match(etag.to_string(), match_client_etags) {
                return Err(format!("if-none-match '{}' matched etag '{}'", if_none_match, etag));
            }
        }
    }

    if let Some(header) = headers.get("if-modified-since") {
        if let Some(client_modified_string) = header {
            if let Ok(client_modified) =
                client_modified_string.parse::<types::Timestamptz>()
            {
                if !check_if_modified(last_modified, client_modified) {
                    return Err(format!(
                        "object was modified at '{}'; if-modified-since '{}'",
                        last_modified.to_rfc3339(), client_modified.to_rfc3339(),
                    ));
                }
            } else {
                return Err(format!(
                    "unable to parse '{}' as a valid date",
                    client_modified_string,
                ));
            }
        }
    }

    Ok(())
}

fn check_if_match(etag: String, client_etags: Vec<&str>) -> bool {
    // XXX
    //
    // - How is this handling whitespace around the comma?
    // - Are we still ignoring weak comparisons?
    for client_etag in client_etags {
        let client_etag = client_etag.replace("\"", "");
        if client_etag == "*" || etag == client_etag {
            return true;
        }
    }

    false
}

fn check_if_modified(
    last_modified: types::Timestamptz,
    client_modified: types::Timestamptz,
) -> bool {
    /*
     * XXX What about timestamps that are exactly the same?
     */
    last_modified > client_modified
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck::quickcheck;
    use std::collections::HashMap;
    use chrono;

    use crate::object::ObjectResponse;

    /*
     * XXX Somehow we get object.rs's Arbitrary implementation for free?
     */

    #[test]
    fn precon_empty_headers() {
        let h = Some(HashMap::new());
        assert_eq!(is_conditional(&h), false);
    }

    #[test]
    fn precon_none_headers() {
        let h = None;
        assert_eq!(is_conditional(&h), false);
    }

    #[test]
    fn precon_not_applicable_headers() {
        let mut h = HashMap::new();
        let _ = h.insert("if-something".into(), Some("test".into()));
        let _ = h.insert("accept".into(), Some("test".into()));
        let h = Some(h);
        assert_eq!(is_conditional(&h), false);
    }

    #[test]
    fn precon_mix_headers() {
        let mut h = HashMap::new();
        let _ = h.insert("if-match".into(), Some("test".into()));
        let _ = h.insert("if-modified-since".into(), Some("test".into()));
        let _ = h.insert("if-none-match".into(), Some("test".into()));
        let h = Some(h);
        assert_eq!(is_conditional(&h), true);
    }

    /*
     * if-match
     */
    quickcheck! {
        fn precon_check_if_match_single(res: ObjectResponse) -> () {
            let mut h = HashMap::new();
            let _ = h.insert("if-match".into(), Some(res.id.to_string()));

            assert!(check_conditional(&h, res.id, res.modified).is_ok());
        }
    }
    quickcheck! {
        fn precon_check_if_match_list(res: ObjectResponse) -> () {
            let mut h = HashMap::new();
            let _ = h.insert("if-match".into(), Some(format!("thing,\"{}\"", res.id)));

            assert!(check_conditional(&h, res.id, res.modified).is_ok());
        }
    }
    quickcheck! {
        fn precon_check_if_match_list_with_any(res: ObjectResponse) -> () {
            let mut h = HashMap::new();
            let _ = h.insert("if-match".into(), Some("\"test\",thing,*".into()));

            assert!(check_conditional(&h, res.id, res.modified).is_ok());
        }
    }
    quickcheck! {
        fn precon_check_if_match_any(res: ObjectResponse) -> () {
            let mut h = HashMap::new();
            let _ = h.insert("if-match".into(), Some("*".into()));

            assert!(check_conditional(&h, res.id, res.modified).is_ok());
        }
    }
    quickcheck! {
        fn precon_check_if_match_single_fail(res: ObjectResponse) -> () {
            let client_etag = Uuid::new_v4();

            let mut h = HashMap::new();
            let _ = h.insert("if-match".into(), Some(client_etag.to_string()));

            let check_res = check_conditional(&h, res.id, res.modified);

            assert!(check_res.is_err());
            assert_eq!(
                check_res.unwrap_err(),
                format!("if-match '{}' didn't match etag '{}'", client_etag, res.id),
            );
        }
    }

    /*
     * if-none-match
     */
    quickcheck! {
        fn precon_check_if_none_match_single(res: ObjectResponse) -> () {
            let client_etag = Uuid::new_v4();

            let mut h = HashMap::new();
            let _ = h.insert("if-none-match".into(), Some(client_etag.to_string()));

            assert!(check_conditional(&h, res.id, res.modified).is_ok());
        }
    }
    quickcheck! {
        fn precon_check_if_none_match_list_fail(res: ObjectResponse) -> () {
            let mut h = HashMap::new();
            let client_etag = format!("thing,\"{}\"", res.id);
            let _ = h.insert("if-none-match".into(), Some(client_etag.clone()));

            let check_res = check_conditional(&h, res.id, res.modified);

            assert!(check_res.is_err());
            assert_eq!(
                check_res.unwrap_err(),
                format!("if-none-match '{}' matched etag '{}'", client_etag, res.id),
            );
        }
    }
    quickcheck! {
        fn precon_check_if_none_match_list_with_any_fail(res: ObjectResponse) -> () {
            let mut h = HashMap::new();
            let client_etag = "\"test\",thing,*";
            let _ = h.insert("if-none-match".into(), Some(client_etag.into()));

            let check_res = check_conditional(&h, res.id, res.modified);

            assert!(check_res.is_err());
            assert_eq!(
                check_res.unwrap_err(),
                format!("if-none-match '{}' matched etag '{}'", client_etag, res.id),
            );
        }
    }

    /*
     * if-modified-since
     */
    quickcheck! {
        fn precon_check_if_modified(res: ObjectResponse) -> () {
            let client_modified = "2000-01-01T10:00:00Z";

            let mut h = HashMap::new();
            let _ = h.insert(
                "if-modified-since".into(),
                Some(client_modified.into()),
            );

            assert!(check_conditional(&h, res.id, res.modified).is_ok());
        }
    }
    quickcheck! {
        fn precon_check_if_modified_fail(res: ObjectResponse) -> () {
            let client_modified: types::Timestamptz =
                chrono::Utc::now() + chrono::Duration::days(1);

            let mut h = HashMap::new();
            let _ = h.insert(
                "if-modified-since".into(),
                Some(format!("{}", client_modified.to_rfc3339())),
            );

            let check_res = check_conditional(&h, res.id, res.modified);

            assert!(check_res.is_err());
            assert_eq!(
                check_res.unwrap_err(),
                format!(
                    "object was modified at '{}'; if-modified-since '{}'",
                    res.modified.to_rfc3339(), client_modified.to_rfc3339(),
                ),
            );
        }
    }
    quickcheck! {
        fn precon_check_if_modified_fail_invalid_date(res: ObjectResponse) -> () {
            let client_modified = "not a valid date";

            let mut h = HashMap::new();
            let _ = h.insert(
                "if-modified-since".into(),
                Some(client_modified.to_string()),
            );

            let check_res = check_conditional(&h, res.id, res.modified);

            assert!(check_res.is_err());
            assert_eq!(
                check_res.unwrap_err(),
                format!("unable to parse '{}' as a valid date", client_modified),
            );
        }
    }

    /*
     * if-unmodified-since
     */
    quickcheck! {
        fn precon_check_if_unmodified(res: ObjectResponse) -> () {
            let client_modified: types::Timestamptz =
                chrono::Utc::now() + chrono::Duration::days(1);

            let mut h = HashMap::new();
            let _ = h.insert(
                "if-unmodified-since".into(),
                Some(format!("{}", client_modified.to_rfc3339())),
            );

            assert!(check_conditional(&h, res.id, res.modified).is_ok());
        }
    }
    quickcheck! {
        fn precon_check_if_unmodified_fail(res: ObjectResponse) -> () {
            let client_modified = "2010-01-01T10:00:00Z";

            let mut h = HashMap::new();
            let _ = h.insert("if-unmodified-since".into(), Some(client_modified.into()));

            let check_res = check_conditional(&h, res.id, res.modified);

            assert!(check_res.is_err());
            assert_eq!(
                check_res.unwrap_err(),
                format!(
                    "object was modified at '{}'; if-unmodified-since '2010-01-01T10:00:00+00:00'",
                    res.modified.to_rfc3339(),
                ),
            );
        }
    }

    /*
     * mixture of match and modified preconditions.
     */
}
