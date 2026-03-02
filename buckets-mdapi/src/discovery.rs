// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright 2026 Edgecast Cloud LLC.

use serde_derive::{Deserialize, Serialize};
use serde_json::Error as SerdeError;
use serde_json::Value;
use slog::{debug, error, Logger};
use uuid::Uuid;

use cueball_postgres_connection::PostgresConnection;
use fast_rpc::protocol::{FastMessage, FastMessageData};

use crate::error::BucketsMdapiError;
use crate::metrics::RegisteredMetrics;
use crate::sql;
use crate::types::{HandlerResponse, HasRequestId};
use crate::util::array_wrap;

// ---------------------------------------------------------------------------
// listvnodes
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ListVnodesPayload {
    pub request_id: Uuid,
}

impl HasRequestId for ListVnodesPayload {
    fn request_id(&self) -> Uuid {
        self.request_id
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ListVnodesResponse {
    pub vnodes: Vec<u64>,
}

pub(crate) fn decode_listvnodes_msg(
    value: &Value,
) -> Result<Vec<ListVnodesPayload>, SerdeError> {
    serde_json::from_value::<Vec<ListVnodesPayload>>(value.clone())
}

// ---------------------------------------------------------------------------
// listowners
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ListOwnersPayload {
    pub vnode: u64,
    pub request_id: Uuid,
}

impl HasRequestId for ListOwnersPayload {
    fn request_id(&self) -> Uuid {
        self.request_id
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ListOwnersResponse {
    pub owners: Vec<Uuid>,
}

pub(crate) fn decode_listowners_msg(
    value: &Value,
) -> Result<Vec<ListOwnersPayload>, SerdeError> {
    serde_json::from_value::<Vec<ListOwnersPayload>>(value.clone())
}

// ---------------------------------------------------------------------------
// listvnodes action
// ---------------------------------------------------------------------------

/// Schema prefix for per-vnode partitions (e.g. `manta_bucket_0`).
const SCHEMA_PREFIX: &str = "manta_bucket_";

// PostgreSQL does not support parameterised identifiers (schema names),
// so we use format!.  Both interpolated values are compile-time constants.
fn listvnodes_sql() -> String {
    format!(
        "SELECT schema_name \
         FROM information_schema.schemata \
         WHERE left(schema_name, {}) = '{}' \
         ORDER BY schema_name",
        SCHEMA_PREFIX.len(),
        SCHEMA_PREFIX
    )
}

fn parse_vnode_from_schema(schema_name: &str) -> Option<u64> {
    if schema_name.starts_with(SCHEMA_PREFIX) {
        schema_name[SCHEMA_PREFIX.len()..].parse::<u64>().ok()
    } else {
        None
    }
}

fn listvnodes_response_to_json(resp: ListVnodesResponse) -> Value {
    serde_json::to_value(resp).expect("failed to serialize ListVnodesResponse")
}

fn do_listvnodes(
    conn: &mut PostgresConnection,
    metrics: &RegisteredMetrics,
    log: &Logger,
) -> Result<ListVnodesResponse, String> {
    let sql = listvnodes_sql();

    sql::query(
        sql::Method::ListVnodes,
        conn,
        sql.as_str(),
        &[],
        metrics,
        log,
    )
    .map_err(|e| e.to_string())
    .map(|rows| {
        // SQL already returns rows ORDER BY schema_name
        let vnodes: Vec<u64> = rows
            .iter()
            .filter_map(|row| {
                let name: String = row.get("schema_name");
                parse_vnode_from_schema(&name)
            })
            .collect();
        ListVnodesResponse { vnodes }
    })
}

#[allow(clippy::needless_pass_by_value)]
pub(crate) fn listvnodes_action(
    msg_id: u32,
    method: &str,
    metrics: &RegisteredMetrics,
    log: &Logger,
    _payload: ListVnodesPayload,
    conn: &mut PostgresConnection,
) -> Result<HandlerResponse, String> {
    do_listvnodes(conn, metrics, log)
        .map(|resp| {
            debug!(log, "operation successful");
            let value = listvnodes_response_to_json(resp);
            let msg_data =
                FastMessageData::new(method.into(), array_wrap(value));
            FastMessage::data(msg_id, msg_data).into()
        })
        .or_else(|e| {
            error!(log, "operation failed"; "error" => &e);
            let err = BucketsMdapiError::PostgresError(e);
            let msg_data = FastMessageData::new(
                method.into(),
                array_wrap(err.into_fast()),
            );
            let msg: HandlerResponse =
                FastMessage::data(msg_id, msg_data).into();
            Ok(msg)
        })
}

// ---------------------------------------------------------------------------
// listowners action
// ---------------------------------------------------------------------------

// PostgreSQL does not support parameterised identifiers (schema names),
// so we use format!.  `vnode` is a u64, so no injection risk.
fn listowners_sql(vnode: u64) -> String {
    format!(
        "SELECT DISTINCT owner \
         FROM {}{}.manta_bucket",
        SCHEMA_PREFIX, vnode
    )
}

/// Lightweight single-row check for vnode schema existence, avoiding
/// the full table scan that `do_listvnodes` performs.
fn vnode_schema_exists(
    vnode: u64,
    conn: &mut PostgresConnection,
    metrics: &RegisteredMetrics,
    log: &Logger,
) -> Result<bool, String> {
    let schema_name = format!("{}{}", SCHEMA_PREFIX, vnode);
    let sql = "SELECT 1 FROM information_schema.schemata \
               WHERE schema_name = $1 LIMIT 1";

    sql::query(
        sql::Method::VnodeExists,
        conn,
        sql,
        &[&schema_name],
        metrics,
        log,
    )
    .map_err(|e| e.to_string())
    .map(|rows| !rows.is_empty())
}

fn listowners_response_to_json(resp: ListOwnersResponse) -> Value {
    serde_json::to_value(resp).expect("failed to serialize ListOwnersResponse")
}

fn do_listowners(
    vnode: u64,
    conn: &mut PostgresConnection,
    metrics: &RegisteredMetrics,
    log: &Logger,
) -> Result<ListOwnersResponse, String> {
    let sql = listowners_sql(vnode);

    sql::query(
        sql::Method::ListOwners,
        conn,
        sql.as_str(),
        &[],
        metrics,
        log,
    )
    .map_err(|e| e.to_string())
    .map(|rows| {
        let owners: Vec<Uuid> =
            rows.iter().map(|row| row.get("owner")).collect();
        ListOwnersResponse { owners }
    })
}

#[allow(clippy::needless_pass_by_value)]
pub(crate) fn listowners_action(
    msg_id: u32,
    method: &str,
    metrics: &RegisteredMetrics,
    log: &Logger,
    payload: ListOwnersPayload,
    conn: &mut PostgresConnection,
) -> Result<HandlerResponse, String> {
    // Validate that the requested vnode schema exists before
    // querying it, to return a clean application-level error
    // instead of a raw PostgreSQL "relation does not exist".
    match vnode_schema_exists(payload.vnode, conn, metrics, log) {
        Ok(false) => {
            let e = format!(
                "vnode {} does not exist on this mdapi instance",
                payload.vnode
            );
            error!(log, "operation failed"; "error" => &e);
            let err = BucketsMdapiError::PostgresError(e);
            let msg_data = FastMessageData::new(
                method.into(),
                array_wrap(err.into_fast()),
            );
            let msg: HandlerResponse =
                FastMessage::data(msg_id, msg_data).into();
            return Ok(msg);
        }
        Err(e) => {
            error!(log, "operation failed"; "error" => &e);
            let err = BucketsMdapiError::PostgresError(e);
            let msg_data = FastMessageData::new(
                method.into(),
                array_wrap(err.into_fast()),
            );
            let msg: HandlerResponse =
                FastMessage::data(msg_id, msg_data).into();
            return Ok(msg);
        }
        Ok(true) => {} // vnode exists, proceed
    }

    do_listowners(payload.vnode, conn, metrics, log)
        .map(|resp| {
            debug!(log, "operation successful");
            let value = listowners_response_to_json(resp);
            let msg_data =
                FastMessageData::new(method.into(), array_wrap(value));
            FastMessage::data(msg_id, msg_data).into()
        })
        .or_else(|e| {
            error!(log, "operation failed"; "error" => &e);
            let err = BucketsMdapiError::PostgresError(e);
            let msg_data = FastMessageData::new(
                method.into(),
                array_wrap(err.into_fast()),
            );
            let msg: HandlerResponse =
                FastMessage::data(msg_id, msg_data).into();
            Ok(msg)
        })
}

#[cfg(test)]
mod test {
    use super::*;

    use quickcheck::{quickcheck, Arbitrary, Gen};
    use serde_json;
    use serde_json::Map;

    // -- ListVnodesPayload ---------------------------------------------------

    #[derive(Clone, Debug)]
    struct ListVnodesJson(Value);

    impl Arbitrary for ListVnodesJson {
        fn arbitrary<G: Gen>(_g: &mut G) -> Self {
            let request_id = serde_json::to_value(Uuid::new_v4())
                .expect("failed to convert request_id field to Value");

            let mut obj = Map::new();
            obj.insert("request_id".into(), request_id);
            ListVnodesJson(Value::Object(obj))
        }
    }

    impl Arbitrary for ListVnodesPayload {
        fn arbitrary<G: Gen>(_g: &mut G) -> Self {
            ListVnodesPayload {
                request_id: Uuid::new_v4(),
            }
        }
    }

    // -- ListOwnersPayload ---------------------------------------------------

    #[derive(Clone, Debug)]
    struct ListOwnersJson(Value);

    impl Arbitrary for ListOwnersJson {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let vnode = serde_json::to_value(u64::arbitrary(g))
                .expect("failed to convert vnode field to Value");
            let request_id = serde_json::to_value(Uuid::new_v4())
                .expect("failed to convert request_id field to Value");

            let mut obj = Map::new();
            obj.insert("vnode".into(), vnode);
            obj.insert("request_id".into(), request_id);
            ListOwnersJson(Value::Object(obj))
        }
    }

    impl Arbitrary for ListOwnersPayload {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            ListOwnersPayload {
                vnode: u64::arbitrary(g),
                request_id: Uuid::new_v4(),
            }
        }
    }

    // -- ListVnodesResponse --------------------------------------------------

    impl Arbitrary for ListVnodesResponse {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let count = usize::arbitrary(g) % 16;
            let vnodes = (0..count).map(|_| u64::arbitrary(g)).collect();
            ListVnodesResponse { vnodes }
        }
    }

    // -- ListOwnersResponse --------------------------------------------------

    impl Arbitrary for ListOwnersResponse {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let count = usize::arbitrary(g) % 16;
            let owners = (0..count).map(|_| Uuid::new_v4()).collect();
            ListOwnersResponse { owners }
        }
    }

    // -- Unit tests for SQL and parsing --------------------------------------

    #[test]
    fn test_parse_vnode_from_schema() {
        assert_eq!(parse_vnode_from_schema("manta_bucket_0"), Some(0));
        assert_eq!(parse_vnode_from_schema("manta_bucket_42"), Some(42));
        assert_eq!(parse_vnode_from_schema("manta_bucket_1023"), Some(1023));
        assert_eq!(parse_vnode_from_schema("public"), None);
        assert_eq!(parse_vnode_from_schema("manta_bucket_"), None);
        assert_eq!(parse_vnode_from_schema("manta_bucket_abc"), None);
    }

    #[test]
    fn test_listvnodes_sql_contains_schema_prefix() {
        let sql = listvnodes_sql();
        assert!(sql.contains("information_schema.schemata"));
        assert!(sql.contains("manta_bucket_"));
    }

    #[test]
    fn test_listowners_sql_contains_vnode() {
        let sql = listowners_sql(7);
        assert!(sql.contains("manta_bucket_7.manta_bucket"));
        assert!(sql.contains("DISTINCT owner"));
    }

    // -- Quickcheck properties -----------------------------------------------

    quickcheck! {
        fn prop_listvnodes_payload_serialize_deserialize_identity(msg: ListVnodesPayload) -> bool {
            match serde_json::to_string(&msg) {
                Ok(s) => {
                    let decode_result: Result<ListVnodesPayload, _> =
                        serde_json::from_str(&s);
                    match decode_result {
                        Ok(decoded) => decoded == msg,
                        Err(_) => false,
                    }
                }
                Err(_) => false,
            }
        }
    }

    quickcheck! {
        fn prop_listowners_payload_serialize_deserialize_identity(msg: ListOwnersPayload) -> bool {
            match serde_json::to_string(&msg) {
                Ok(s) => {
                    let decode_result: Result<ListOwnersPayload, _> =
                        serde_json::from_str(&s);
                    match decode_result {
                        Ok(decoded) => decoded == msg,
                        Err(_) => false,
                    }
                }
                Err(_) => false,
            }
        }
    }

    quickcheck! {
        fn prop_listvnodes_response_serialize_deserialize_identity(msg: ListVnodesResponse) -> bool {
            match serde_json::to_string(&msg) {
                Ok(s) => {
                    let decode_result: Result<ListVnodesResponse, _> =
                        serde_json::from_str(&s);
                    match decode_result {
                        Ok(decoded) => decoded == msg,
                        Err(_) => false,
                    }
                }
                Err(_) => false,
            }
        }
    }

    quickcheck! {
        fn prop_listowners_response_serialize_deserialize_identity(msg: ListOwnersResponse) -> bool {
            match serde_json::to_string(&msg) {
                Ok(s) => {
                    let decode_result: Result<ListOwnersResponse, _> =
                        serde_json::from_str(&s);
                    match decode_result {
                        Ok(decoded) => decoded == msg,
                        Err(_) => false,
                    }
                }
                Err(_) => false,
            }
        }
    }

    quickcheck! {
        fn prop_listvnodes_payload_from_json(json: ListVnodesJson) -> bool {
            let decode_result1: Result<ListVnodesPayload, _> =
                serde_json::from_value(json.0.clone());
            let res1 = decode_result1.is_ok();

            let decode_result2: Result<Vec<ListVnodesPayload>, _> =
                serde_json::from_value(Value::Array(vec![json.0]));
            let res2 = decode_result2.is_ok();

            res1 && res2
        }
    }

    quickcheck! {
        fn prop_listowners_payload_from_json(json: ListOwnersJson) -> bool {
            let decode_result1: Result<ListOwnersPayload, _> =
                serde_json::from_value(json.0.clone());
            let res1 = decode_result1.is_ok();

            let decode_result2: Result<Vec<ListOwnersPayload>, _> =
                serde_json::from_value(Value::Array(vec![json.0]));
            let res2 = decode_result2.is_ok();

            res1 && res2
        }
    }
}
