// Copyright 2020 Joyent, Inc.

use postgres::types::ToSql;
use postgres::Transaction;
use slog::{trace, Logger};
use serde_derive::{Deserialize, Serialize};

use crate::error::BucketsMdapiError;
use crate::metrics;
use crate::object::{get_sql, response, ObjectResponse};
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

impl Conditions {
    fn is_conditional(&self) -> bool {
        self.if_match.is_some()
            || self.if_none_match.is_some()
            || self.if_modified_since.is_some()
            || self.if_unmodified_since.is_some()
    }

    pub fn check(
        &self,
        maybe_object: Option<&ObjectResponse>,
    ) -> Result<(), BucketsMdapiError> {
        let object = match maybe_object {
            None => {
                if let Some(client_etags) = &self.if_match {
                    if check_if_match_wildcard(client_etags) {
                        return Err(error(
                            format!("if-match '{}' matched a non-existent object",
                                print_etags(&client_etags)),
                        ));
                    }
                }

                if let Some(client_etags) = &self.if_none_match {
                    if check_if_match_wildcard(client_etags) {
                        return Ok(());
                    }
                }

                return Err(BucketsMdapiError::ObjectNotFound);
            },
            Some(object) => object,
        };

        let etag = object.id.to_string();
        let last_modified = object.modified;

        if let Some(client_etags) = &self.if_match {
            if !check_if_match(&etag, client_etags) {
                return Err(error(
                    format!("if-match '{}' didn't match etag '{}'",
                        print_etags(&client_etags), etag),
                ));
            }
        }

        if let Some(client_unmodified) = self.if_unmodified_since {
            if last_modified > client_unmodified {
                return Err(error(
                    format!(
                        "object was modified at '{}'; if-unmodified-since '{}'",
                        last_modified.to_rfc3339(), client_unmodified.to_rfc3339(),
                    ),
                ));
            }
        }

        if let Some(client_etags) = &self.if_none_match {
            if check_if_match(&etag, client_etags) {
                return Err(error(
                    format!("if-none-match '{}' matched etag '{}'",
                        print_etags(&client_etags), etag),
                ));
            }
        }

        if let Some(client_modified) = self.if_modified_since {
            if last_modified <= client_modified {
                return Err(error(
                    format!(
                        "object was modified at '{}'; if-modified-since '{}'",
                        last_modified.to_rfc3339(), client_modified.to_rfc3339(),
                    ),
                ));
            }
        }

        Ok(())
    }
}

fn error(msg: String) -> BucketsMdapiError {
    BucketsMdapiError::PreconditionFailedError(msg)
}

pub fn request(
    mut txn: &mut Transaction,
    items: &[&dyn ToSql],
    vnode: u64,
    conditions: &Conditions,
    metrics: &metrics::RegisteredMetrics,
    log: &Logger,
) -> Result<(), BucketsMdapiError> {
    if !conditions.is_conditional() {
        trace!(log, "request not conditional; returning");
        return Ok(());
    }

    sql::txn_query(
        sql::Method::ObjectGet,
        &mut txn,
        get_sql(vnode).as_str(),
        items,
        metrics,
        log,
    )
    .map_err(|e| { BucketsMdapiError::PostgresError(e.to_string()) })
    .and_then(|rows| { response("getobject", &rows) })
    .and_then(|maybe_resp| { conditions.check(maybe_resp.as_ref()) })
}

fn check_if_match_wildcard(client_etags: &[String]) -> bool {
    client_etags.iter().any(|x| x == "*")
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
    use uuid::Uuid;

    fn conditions_from_value(v: Value) -> Conditions {
        serde_json::from_value::<Conditions>(v).unwrap()
    }

    #[test]
    fn precon_empty_headers() {
        let h = conditions_from_value(json!({}));
        assert_eq!(h.is_conditional(), false);
    }

    #[test]
    fn precon_undefined_headers() {
        let h = conditions_from_value(json!({
            "if-modified-since": null,
        }));
        assert_eq!(h.is_conditional(), false);
    }

    #[test]
    fn precon_not_applicable_headers() {
        let h = conditions_from_value(json!({
            "if-something": [ "test" ],
            "accept": [ "test" ],
        }));
        assert_eq!(h.is_conditional(), false);
    }

    #[test]
    fn precon_mix_headers() {
        let h = conditions_from_value(json!({
            "if-match": [ "test" ],
            "if-modified-since": "2020-10-01T10:00:00Z",
            "if-none-match": [ "test" ],
        }));
        assert_eq!(h.is_conditional(), true);
    }

    /*
     * if-match
     */
    quickcheck! {
        fn precon_check_if_match_single(res: ObjectResponse) -> () {
            let h = conditions_from_value(json!({
                "if-match": [ res.id ],
            }));

            assert!(h.check(Some(&res)).is_ok());
        }
    }
    quickcheck! {
        fn precon_check_if_match_list(res: ObjectResponse) -> () {
            let h = conditions_from_value(json!({
                "if-match": [ "thing", res.id ],
            }));

            assert!(h.check(Some(&res)).is_ok());
        }
    }
    quickcheck! {
        fn precon_check_if_match_list_with_any(res: ObjectResponse) -> () {
            let h = conditions_from_value(json!({
                "if-match": [ "test", "thing", "*" ],
            }));

            assert!(h.check(Some(&res)).is_ok());
        }
    }
    quickcheck! {
        fn precon_check_if_match_any(res: ObjectResponse) -> () {
            let h = conditions_from_value(json!({
                "if-match": [ "*" ],
            }));

            assert!(h.check(Some(&res)).is_ok());
        }
    }
    quickcheck! {
        fn precon_check_if_match_single_fail(res: ObjectResponse) -> () {
            let client_etag = Uuid::new_v4();

            let h = conditions_from_value(json!({
                "if-match": [ client_etag ],
            }));

            let check_res = h.check(Some(&res));

            assert!(check_res.is_err());
            let err = check_res.unwrap_err();
            assert_eq!(
                err.message(),
                format!("if-match '\"{}\"' didn't match etag '{}'", client_etag, res.id),
            );
            assert_eq!(
                err.to_string(),
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

            assert!(h.check(Some(&res)).is_ok());
        }
    }
    quickcheck! {
        fn precon_check_if_none_match_list_fail(res: ObjectResponse) -> () {
            let h = conditions_from_value(json!({
                "if-none-match": [ "test", "thing", res.id ],
            }));

            let check_res = h.check(Some(&res));

            assert!(check_res.is_err());
            let err = check_res.unwrap_err();
            assert_eq!(
                err.message(),
                format!("if-none-match '\"test\", \"thing\", \"{}\"' matched etag '{}'",
                    res.id, res.id),
            );
            assert_eq!(
                err.to_string(),
                "PreconditionFailedError".to_string(),
            );
        }
    }
    quickcheck! {
        fn precon_check_if_none_match_list_with_any_fail(res: ObjectResponse) -> () {
            let h = conditions_from_value(json!({
                "if-none-match": [ "test", "thing", "*" ],
            }));

            let check_res = h.check(Some(&res));

            assert!(check_res.is_err());
            let err = check_res.unwrap_err();
            assert_eq!(
                err.message(),
                format!("if-none-match '\"test\", \"thing\", \"*\"' matched etag '{}'", res.id),
            );
            assert_eq!(
                err.to_string(),
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

            assert!(h.check(Some(&res)).is_ok());
        }
    }
    quickcheck! {
        fn precon_check_if_modified_fail(res: ObjectResponse) -> () {
            let client_modified: types::Timestamptz =
                chrono::Utc::now() + chrono::Duration::days(1);

            let h = conditions_from_value(json!({
                "if-modified-since": client_modified.to_rfc3339(),
            }));

            let check_res = h.check(Some(&res));

            assert!(check_res.is_err());
            let err = check_res.unwrap_err();
            assert_eq!(
                err.message(),
                format!(
                    "object was modified at '{}'; if-modified-since '{}'",
                    res.modified.to_rfc3339(), client_modified.to_rfc3339(),
                ),
            );
            assert_eq!(
                err.to_string(),
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

            assert!(h.check(Some(&res)).is_ok());
        }
    }
    quickcheck! {
        fn precon_check_if_unmodified_fail(res: ObjectResponse) -> () {
            let client_modified = "2010-01-01T10:00:00Z";

            let h = conditions_from_value(json!({
                "if-unmodified-since": client_modified,
            }));

            let check_res = h.check(Some(&res));

            assert!(check_res.is_err());
            let err = check_res.unwrap_err();
            assert_eq!(
                err.message(),
                format!(
                    "object was modified at '{}'; if-unmodified-since '2010-01-01T10:00:00+00:00'",
                    res.modified.to_rfc3339(),
                ),
            );
        }
    }
}
