// Copyright 2020 Joyent, Inc.

use serde_derive::{Deserialize, Serialize};
use serde_json::Error as SerdeError;
use serde_json::Value;
use slog::{debug, error, Logger};
use uuid::Uuid;

use cueball_postgres_connection::PostgresConnection;
use fast_rpc::protocol::{FastMessage, FastMessageData};

use crate::bucket::{to_json, BucketResponse};
use crate::metrics::RegisteredMetrics;
use crate::sql;
use crate::types::{HandlerResponse, HasRequestId};
use crate::util::{array_wrap, limit_constraint_error};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ListBucketsPayload {
    pub owner: Uuid,
    pub vnode: u64,
    pub prefix: Option<String>,
    pub limit: u64,
    pub marker: Option<String>,
    pub request_id: Uuid,
}

impl HasRequestId for ListBucketsPayload {
    fn request_id(&self) -> Uuid {
        self.request_id
    }
}

pub(crate) fn decode_msg(
    value: &Value,
) -> Result<Vec<ListBucketsPayload>, SerdeError> {
    serde_json::from_value::<Vec<ListBucketsPayload>>(value.clone())
}

pub(crate) fn action(
    msg_id: u32,
    method: &str,
    metrics: &RegisteredMetrics,
    log: &Logger,
    payload: ListBucketsPayload,
    conn: &mut PostgresConnection,
) -> Result<HandlerResponse, String> {
    // Make database request
    if payload.limit > 0 && payload.limit <= 1024 {
        do_list(msg_id, method, payload, conn, metrics, log)
            .and_then(|resp| {
                // Handle the successful database response
                debug!(log, "operation successful");
                Ok(HandlerResponse::from(resp))
            })
            .or_else(|e| {
                // Handle database error response
                error!(log, "operation failed"; "error" => &e);

                // Database errors are returned to as regular Fast messages
                // to be handled by the calling application
                let value = sql::postgres_error(e);
                let msg_data =
                    FastMessageData::new(method.into(), array_wrap(value));
                let msg: HandlerResponse =
                    FastMessage::data(msg_id, msg_data).into();
                Ok(msg)
            })
    } else {
        // Limit constraint violations are returned to as regular
        // Fast messages to be handled by the calling application
        let e = format!(
            "the {} limit option must be a value between 1 \
             and 1024. the requested limit was {}",
            &method, &payload.limit
        );
        let value = limit_constraint_error(e);
        let msg_data = FastMessageData::new(method.into(), array_wrap(value));
        let msg: HandlerResponse = FastMessage::data(msg_id, msg_data).into();
        Ok(msg)
    }
}

fn do_list(
    msg_id: u32,
    method: &str,
    payload: ListBucketsPayload,
    mut conn: &mut PostgresConnection,
    metrics: &RegisteredMetrics,
    log: &Logger,
) -> Result<Vec<FastMessage>, String> {
    let query_result = match (payload.marker, payload.prefix) {
        (Some(marker), Some(prefix)) => {
            let sql = list_sql_prefix_marker(payload.vnode, payload.limit);
            let prefix = format!("{}%", prefix);
            sql::query(
                sql::Method::BucketList,
                &mut conn,
                sql.as_str(),
                &[&payload.owner, &prefix, &marker],
                metrics,
                log,
            )
        }
        (Some(marker), None) => {
            let sql = list_sql_marker(payload.vnode, payload.limit);
            sql::query(
                sql::Method::BucketList,
                &mut conn,
                sql.as_str(),
                &[&payload.owner, &marker],
                metrics,
                log,
            )
        }
        (None, Some(prefix)) => {
            let sql = list_sql_prefix(payload.vnode, payload.limit);
            let prefix = format!("{}%", prefix);
            sql::query(
                sql::Method::BucketList,
                &mut conn,
                sql.as_str(),
                &[&payload.owner, &prefix],
                metrics,
                log,
            )
        }
        (None, None) => {
            let sql = list_sql(payload.vnode, payload.limit);
            sql::query(
                sql::Method::BucketList,
                &mut conn,
                sql.as_str(),
                &[&payload.owner],
                metrics,
                log,
            )
        }
    };

    let mut msgs: Vec<FastMessage> = Vec::with_capacity(1024);

    query_result.map_err(|e| e.to_string()).and_then(|rows| {
        for row in &rows {
            let resp = BucketResponse {
                id: row.get("id"),
                owner: row.get("owner"),
                name: row.get("name"),
                created: row.get("created"),
            };

            let value = to_json(resp);
            let msg_data =
                FastMessageData::new(method.into(), array_wrap(value));
            let msg = FastMessage::data(msg_id, msg_data);

            msgs.push(msg);
        }
        Ok(msgs)
    })
}

fn list_sql_prefix_marker(vnode: u64, limit: u64) -> String {
    format!(
        "SELECT id, owner, name, created
        FROM manta_bucket_{}.manta_bucket
        WHERE owner = $1 AND name like $2 AND name > $3
        ORDER BY name ASC
        LIMIT {}",
        vnode, limit
    )
}

fn list_sql_prefix(vnode: u64, limit: u64) -> String {
    format!(
        "SELECT id, owner, name, created
        FROM manta_bucket_{}.manta_bucket
        WHERE owner = $1 AND name like $2
        ORDER BY name ASC
        LIMIT {}",
        vnode, limit
    )
}
fn list_sql_marker(vnode: u64, limit: u64) -> String {
    format!(
        "SELECT id, owner, name, created
        FROM manta_bucket_{}.manta_bucket
        WHERE owner = $1 AND name > $2
        ORDER BY name ASC
        LIMIT {}",
        vnode, limit
    )
}

fn list_sql(vnode: u64, limit: u64) -> String {
    format!(
        "SELECT id, owner, name, created
        FROM manta_bucket_{}.manta_bucket
        WHERE owner = $1
        ORDER BY name ASC
        LIMIT {}",
        vnode, limit
    )
}

#[cfg(test)]
mod test {
    use super::*;

    use quickcheck::{quickcheck, Arbitrary, Gen};
    use quickcheck_helpers::random;
    use serde_json;
    use serde_json::Map;

    #[derive(Clone, Debug)]
    struct ListBucketsJson(Value);

    impl Arbitrary for ListBucketsJson {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let owner = serde_json::to_value(Uuid::new_v4())
                .expect("failed to convert owner field to Value");
            let vnode = serde_json::to_value(u64::arbitrary(g))
                .expect("failed to convert vnode field to Value");
            let prefix = serde_json::to_value(random::string(g, 32))
                .expect("failed to convert prefix field to Value");
            let limit = serde_json::to_value(u64::arbitrary(g))
                .expect("failed to convert limit field to Value");
            let marker = serde_json::to_value(random::string(g, 32))
                .expect("failed to convert marker field to Value");
            let request_id = serde_json::to_value(Uuid::new_v4())
                .expect("failed to convert request_id field to Value");

            let mut obj = Map::new();
            obj.insert("owner".into(), owner);
            obj.insert("vnode".into(), vnode);
            obj.insert("prefix".into(), prefix);
            obj.insert("limit".into(), limit);
            obj.insert("marker".into(), marker);
            obj.insert("request_id".into(), request_id);
            ListBucketsJson(Value::Object(obj))
        }
    }

    impl Arbitrary for ListBucketsPayload {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let owner = Uuid::new_v4();
            let vnode = u64::arbitrary(g);
            let prefix = Some(random::string(g, 32));
            let limit = u64::arbitrary(g);
            let marker = Some(random::string(g, 32));
            let request_id = Uuid::new_v4();

            ListBucketsPayload {
                owner,
                vnode,
                prefix,
                limit,
                marker,
                request_id,
            }
        }
    }

    quickcheck! {
        fn prop_list_bucket_payload_roundtrip(msg: ListBucketsPayload) -> bool {
            match serde_json::to_string(&msg) {
                Ok(list_str) => {
                    let decode_result: Result<ListBucketsPayload, _> =
                        serde_json::from_str(&list_str);
                    match decode_result {
                        Ok(decoded_msg) => decoded_msg == msg,
                        Err(_) => false
                    }
                },
                Err(_) => false
            }
        }
    }

    quickcheck! {
        fn prop_listbucket_payload_from_json(json: ListBucketsJson) -> bool {
            let decode_result1: Result<ListBucketsPayload, _> =
                serde_json::from_value(json.0.clone());
            let res1 = decode_result1.is_ok();

            let decode_result2: Result<Vec<ListBucketsPayload>, _> =
                serde_json::from_value(Value::Array(vec![json.0]));
            let res2 = decode_result2.is_ok();

            res1 && res2
        }
    }
}
