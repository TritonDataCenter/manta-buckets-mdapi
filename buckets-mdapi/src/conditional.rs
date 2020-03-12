// Copyright 2020 Joyent, Inc.

use serde_json::Value;
use postgres::types::ToSql;
use postgres::Transaction;
use serde_json;
use slog::{trace, Logger};
use uuid::Uuid;
use serde_derive::{Deserialize, Serialize};

use crate::object;
use crate::error;
use crate::metrics;
use crate::sql;
use crate::types;

#[derive(Default, Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Conditions {
    #[serde(alias = "if-match")]
    pub if_match: Option<Vec<String>>,

    #[serde(alias = "if-none-match")]
    pub if_none_match: Option<Vec<String>>,

    #[serde(alias = "if-modified-since")]
    pub if_modified_since: Option<types::Timestamptz>,

    #[serde(alias = "if-unmodified-since")]
    pub if_unmodified_since: Option<types::Timestamptz>,
}

pub fn error(error_type: error::BucketsMdapiErrorType, msg: String) -> serde_json::Value {
    serde_json::to_value(error::BucketsMdapiError::with_message(
        error_type,
        msg,
    ))
    .expect("failed to encode a PreconditionFailedError error")
}

pub fn request(
    mut txn: &mut Transaction,
    items: &[&dyn ToSql],
    vnode: u64,
    conditions: &Conditions,
    metrics: &metrics::RegisteredMetrics,
    log: &Logger,
) -> Result<(), Value> {
    if !is_conditional(conditions) {
        trace!(log, "request not conditional; returning");
        return Ok(());
    }

    sql::txn_query(
        sql::Method::ObjectGet,
        &mut txn,
        sql::get_sql(vnode).as_str(),
        items,
        metrics,
        log,
    )
    .map_err(|e| { sql::postgres_error(e.to_string()) })
    .and_then(|rows| {
        if rows.is_empty() {
            return Err(object::object_not_found());
        } else if rows.len() > 1 {
            return Err(sql::postgres_error("expected 1 row from conditional query".to_string()));
        }

        let object_id: Uuid = rows[0].get("id");
        let object_modified: types::Timestamptz = rows[0].get("modified");
        check_conditional(&conditions, object_id.to_string(), object_modified)?;

        Ok(())
    })
}

fn is_conditional(conditions: &Conditions) -> bool {
    conditions.if_match.is_some()
        || conditions.if_none_match.is_some()
        || conditions.if_modified_since.is_some()
        || conditions.if_unmodified_since.is_some()
}

fn check_conditional(
    conditions: &Conditions,
    etag: String,
    last_modified: types::Timestamptz,
) -> Result<(), Value> {
    if let Some(client_etags) = &conditions.if_match {
        if !check_if_match(&etag, client_etags) {
            return Err(error(
                error::BucketsMdapiErrorType::PreconditionFailedError,
                format!("if-match '{}' didn't match etag '{}'", print_etags(&client_etags), etag),
            ));
        }
    }

    if let Some(client_unmodified) = conditions.if_unmodified_since {
        if last_modified > client_unmodified {
            return Err(error(
                error::BucketsMdapiErrorType::PreconditionFailedError,
                format!(
                    "object was modified at '{}'; if-unmodified-since '{}'",
                    last_modified.to_rfc3339(), client_unmodified.to_rfc3339(),
                ),
            ));
        }
    }

    if let Some(client_etags) = &conditions.if_none_match {
        if check_if_match(&etag, client_etags) {
            return Err(error(
                error::BucketsMdapiErrorType::PreconditionFailedError,
                format!("if-none-match '{}' matched etag '{}'", print_etags(&client_etags), etag),
            ));
        }
    }

    if let Some(client_modified) = conditions.if_modified_since {
        if last_modified <= client_modified {
            return Err(error(
                error::BucketsMdapiErrorType::PreconditionFailedError,
                format!(
                    "object was modified at '{}'; if-modified-since '{}'",
                    last_modified.to_rfc3339(), client_modified.to_rfc3339(),
                ),
            ));
        }
    }

    Ok(())
}

fn check_if_match(etag: &str, client_etags: &[String]) -> bool {
    for client_etag in client_etags {
        if client_etag == "*" || etag == client_etag {
            return true;
        }
    }

    false
}

fn print_etags(etags: &[String]) -> String {
    etags.iter().map(|e| {
        format!("\"{}\"", e)
    }).collect::<Vec<String>>().join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck::quickcheck;
    use serde_json::{json, Value};
    use chrono;

    use crate::object::ObjectResponse;

    fn conditions_from_value(v: Value) -> Conditions {
        serde_json::from_value::<Conditions>(v).unwrap()
    }

    #[test]
    fn precon_empty_headers() {
        let h = conditions_from_value(json!({}));
        assert_eq!(is_conditional(&h), false);
    }

    #[test]
    fn precon_undefined_headers() {
        let h = conditions_from_value(json!({
            "if-modified-since": null,
        }));
        assert_eq!(is_conditional(&h), false);
    }

    #[test]
    fn precon_not_applicable_headers() {
        let h = conditions_from_value(json!({
            "if-something": [ "test" ],
            "accept": [ "test" ],
        }));
        assert_eq!(is_conditional(&h), false);
    }

    #[test]
    fn precon_mix_headers() {
        let h = conditions_from_value(json!({
            "if-match": [ "test" ],
            "if-modified-since": "2020-10-01T10:00:00Z",
            "if-none-match": [ "test" ],
        }));
        assert_eq!(is_conditional(&h), true);
    }

    /*
     * if-match
     */
    quickcheck! {
        fn precon_check_if_match_single(res: ObjectResponse) -> () {
            let h = conditions_from_value(json!({
                "if-match": [ res.id ],
            }));

            assert!(check_conditional(&h, res.id.to_string(), res.modified).is_ok());
        }
    }
    quickcheck! {
        fn precon_check_if_match_list(res: ObjectResponse) -> () {
            let h = conditions_from_value(json!({
                "if-match": [ "thing", res.id ],
            }));

            assert!(check_conditional(&h, res.id.to_string(), res.modified).is_ok());
        }
    }
    quickcheck! {
        fn precon_check_if_match_list_with_any(res: ObjectResponse) -> () {
            let h = conditions_from_value(json!({
                "if-match": [ "test", "thing", "*" ],
            }));

            assert!(check_conditional(&h, res.id.to_string(), res.modified).is_ok());
        }
    }
    quickcheck! {
        fn precon_check_if_match_any(res: ObjectResponse) -> () {
            let h = conditions_from_value(json!({
                "if-match": [ "*" ],
            }));

            assert!(check_conditional(&h, res.id.to_string(), res.modified).is_ok());
        }
    }
    quickcheck! {
        fn precon_check_if_match_single_fail(res: ObjectResponse) -> () {
            let client_etag = Uuid::new_v4();

            let h = conditions_from_value(json!({
                "if-match": [ client_etag ],
            }));

            println!("{:?}", h);
            let check_res = check_conditional(&h, res.id.to_string(), res.modified);

            assert!(check_res.is_err());
            let err = &check_res.unwrap_err()["error"];
            assert_eq!(
                err["message"],
                format!("if-match '\"{}\"' didn't match etag '{}'", client_etag, res.id),
            );
            assert_eq!(
                err["name"],
                "PreconditionFailedError".to_string(),
            );
        }
    }

    /*
     * if-none-match
     */
    quickcheck! {
        fn precon_check_if_none_match_single(res: ObjectResponse) -> () {
            let client_etag = Uuid::new_v4();

            let h = conditions_from_value(json!({
                "if-none-match": [ client_etag ],
            }));

            assert!(check_conditional(&h, res.id.to_string(), res.modified).is_ok());
        }
    }
    quickcheck! {
        fn precon_check_if_none_match_list_fail(res: ObjectResponse) -> () {
            let h = conditions_from_value(json!({
                "if-none-match": [ "test", "thing", res.id ],
            }));

            let check_res = check_conditional(&h, res.id.to_string(), res.modified);

            assert!(check_res.is_err());
            let err = &check_res.unwrap_err()["error"];
            assert_eq!(
                err["message"],
                format!("if-none-match '\"test\", \"thing\", \"{}\"' matched etag '{}'", res.id, res.id),
            );
            assert_eq!(
                err["name"],
                "PreconditionFailedError".to_string(),
            );
        }
    }
    quickcheck! {
        fn precon_check_if_none_match_list_with_any_fail(res: ObjectResponse) -> () {
            let h = conditions_from_value(json!({
                "if-none-match": [ "test", "thing", "*" ],
            }));

            let check_res = check_conditional(&h, res.id.to_string(), res.modified);

            assert!(check_res.is_err());
            let err = &check_res.unwrap_err()["error"];
            assert_eq!(
                err["message"],
                format!("if-none-match '\"test\", \"thing\", \"*\"' matched etag '{}'", res.id),
            );
            assert_eq!(
                err["name"],
                "PreconditionFailedError".to_string(),
            );
        }
    }

    /*
     * if-modified-since
     */
    quickcheck! {
        fn precon_check_if_modified(res: ObjectResponse) -> () {
            let client_modified = "2000-01-01T10:00:00Z";

            let h = conditions_from_value(json!({
                "if-modified-since": client_modified,
            }));

            assert!(check_conditional(&h, res.id.to_string(), res.modified).is_ok());
        }
    }
    quickcheck! {
        fn precon_check_if_modified_fail(res: ObjectResponse) -> () {
            let client_modified: types::Timestamptz =
                chrono::Utc::now() + chrono::Duration::days(1);

            let h = conditions_from_value(json!({
                "if-modified-since": client_modified.to_rfc3339(),
            }));

            let check_res = check_conditional(&h, res.id.to_string(), res.modified);

            assert!(check_res.is_err());
            let err = &check_res.unwrap_err()["error"];
            assert_eq!(
                err["message"],
                format!(
                    "object was modified at '{}'; if-modified-since '{}'",
                    res.modified.to_rfc3339(), client_modified.to_rfc3339(),
                ),
            );
            assert_eq!(
                err["name"],
                "PreconditionFailedError".to_string(),
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

            let h = conditions_from_value(json!({
                "if-unmodified-since": client_modified.to_rfc3339(),
            }));

            assert!(check_conditional(&h, res.id.to_string(), res.modified).is_ok());
        }
    }
    quickcheck! {
        fn precon_check_if_unmodified_fail(res: ObjectResponse) -> () {
            let client_modified = "2010-01-01T10:00:00Z";

            let h = conditions_from_value(json!({
                "if-unmodified-since": client_modified,
            }));

            let check_res = check_conditional(&h, res.id.to_string(), res.modified);

            assert!(check_res.is_err());
            let err = &check_res.unwrap_err()["error"];
            assert_eq!(
                err["message"],
                format!(
                    "object was modified at '{}'; if-unmodified-since '2010-01-01T10:00:00+00:00'",
                    res.modified.to_rfc3339(),
                ),
            );
            assert_eq!(
                err["name"],
                "PreconditionFailedError".to_string(),
            );
        }
    }
}
