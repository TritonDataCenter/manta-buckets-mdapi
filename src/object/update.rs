// Copyright 2019 Joyent, Inc.

use std::vec::Vec;

use serde_derive::{Deserialize, Serialize};
use serde_json::{json, Value};
use slog::{debug, error, warn, Logger};
use uuid::Uuid;

use cueball_postgres_connection::PostgresConnection;
use rust_fast::protocol::{FastMessage, FastMessageData};

use crate::object::{object_not_found, response, to_json, ObjectResponse};
use crate::sql;
use crate::util::{array_wrap, other_error, HandlerError, HandlerResponse, Hstore};

const METHOD: &str = "updateobject";

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct UpdateObjectPayload {
    pub owner: Uuid,
    pub bucket_id: Uuid,
    pub name: String,
    pub id: Uuid,
    pub vnode: u64,
    pub content_type: String,
    pub headers: Hstore,
    pub properties: Option<Value>,
    pub request_id: Uuid,
}

pub(crate) fn handler(
    msg_id: u32,
    data: &Value,
    mut conn: &mut PostgresConnection,
    log: &Logger,
) -> Result<HandlerResponse, HandlerError> {
    debug!(log, "handling {} function request", &METHOD);

    serde_json::from_value::<Vec<UpdateObjectPayload>>(data.clone())
        .map_err(|e| e.to_string())
        .and_then(|mut arr| {
            // Remove outer JSON array required by Fast
            if !arr.is_empty() {
                Ok(arr.remove(0))
            } else {
                let err_msg = "Failed to parse JSON data as payload for \
                               updateobject function";
                warn!(log, "{}: {}", err_msg, data);
                Err(err_msg.to_string())
            }
        })
        .and_then(|payload| {
            // Make database request
            let req_id = payload.request_id;
            debug!(log, "parsed UpdateObjectPayload, req_id: {}", &req_id);

            update(payload, &mut conn)
                .and_then(|maybe_resp| {
                    // Handle the successful database response
                    debug!(
                        log,
                        "{} operation was successful, req_id: {}", &METHOD, &req_id
                    );
                    let value = match maybe_resp {
                        Some(resp) => to_json(resp),
                        None => object_not_found(),
                    };
                    let msg_data = FastMessageData::new(METHOD.into(), array_wrap(value));
                    let msg: HandlerResponse = FastMessage::data(msg_id, msg_data).into();
                    Ok(msg)
                })
                .or_else(|e| {
                    // Handle database error response
                    error!(
                        log,
                        "{} operation failed: {}, req_id: {}", &METHOD, &e, &req_id
                    );

                    // Database errors are returned to as regular Fast messages
                    // to be handled by the calling application
                    let value = array_wrap(json!({
                        "name": "PostgresError",
                        "message": e
                    }));

                    let msg_data = FastMessageData::new(METHOD.into(), value);
                    let msg: HandlerResponse = FastMessage::data(msg_id, msg_data).into();
                    Ok(msg)
                })
        })
        .map_err(|e| HandlerError::IO(other_error(&e)))
}

fn update(
    payload: UpdateObjectPayload,
    conn: &mut PostgresConnection,
) -> Result<Option<ObjectResponse>, String> {
    let mut txn = (*conn).transaction().map_err(|e| e.to_string())?;
    let update_sql = update_sql(payload.vnode);

    sql::txn_query(
        sql::Method::ObjectUpdate,
        &mut txn,
        update_sql.as_str(),
        &[
            &payload.content_type,
            &payload.headers,
            &payload.properties,
            &payload.owner,
            &payload.bucket_id,
            &payload.name,
        ],
    )
    .and_then(|rows| {
        txn.commit()?;
        Ok(rows)
    })
    .map_err(|e| e.to_string())
    .and_then(|rows| response(METHOD, rows))
}

fn update_sql(vnode: u64) -> String {
    [
        "UPDATE manta_bucket_",
        &vnode.to_string(),
        &".manta_bucket_object \
       SET content_type = $1,
       headers = $2, \
       properties = $3, \
       modified = current_timestamp \
       WHERE owner = $4 \
       AND bucket_id = $5 \
       AND name = $6 \
       RETURNING id, owner, bucket_id, name, created, modified, \
       content_length, content_md5, content_type, headers, \
       sharks, properties",
    ]
    .concat()
}

#[cfg(test)]
mod test {
    use super::*;

    use std::collections::HashMap;

    use quickcheck::{quickcheck, Arbitrary, Gen};
    use quickcheck_helpers::random;
    use serde_json;
    use serde_json::Map;

    #[derive(Clone, Debug)]
    struct UpdateObjectJson(Value);

    impl Arbitrary for UpdateObjectJson {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let owner = serde_json::to_value(Uuid::new_v4())
                .expect("failed to convert owner field to Value");
            let name = serde_json::to_value(random::string(g, 63))
                .expect("failed to convert name field to Value");
            let bucket_id = serde_json::to_value(Uuid::new_v4())
                .expect("failed to convert bucket_id field to Value");
            let id =
                serde_json::to_value(Uuid::new_v4()).expect("failed to convert id field to Value");
            let vnode = serde_json::to_value(u64::arbitrary(g))
                .expect("failed to convert vnode field to Value");
            let content_type = serde_json::to_value(random::string(g, 32))
                .expect("failed to convert content_type field to Value");
            let mut headers = HashMap::new();
            let _ = headers.insert(random::string(g, 32), Some(random::string(g, 32)));
            let _ = headers.insert(random::string(g, 32), Some(random::string(g, 32)));
            let headers =
                serde_json::to_value(headers).expect("failed to convert headers field to Value");
            let request_id = serde_json::to_value(Uuid::new_v4())
                .expect("failed to convert request_id field to Value");

            let mut obj = Map::new();
            obj.insert("owner".into(), owner);
            obj.insert("name".into(), name);
            obj.insert("bucket_id".into(), bucket_id);
            obj.insert("id".into(), id);
            obj.insert("vnode".into(), vnode);
            obj.insert("content_type".into(), content_type);
            obj.insert("headers".into(), headers);
            obj.insert("request_id".into(), request_id);
            UpdateObjectJson(Value::Object(obj))
        }
    }

    impl Arbitrary for UpdateObjectPayload {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let owner = Uuid::new_v4();
            let bucket_id = Uuid::new_v4();
            let name = random::string(g, 32);
            let id = Uuid::new_v4();
            let vnode = u64::arbitrary(g);
            let content_type = random::string(g, 32);
            let mut headers = HashMap::new();
            let _ = headers.insert(random::string(g, 32), Some(random::string(g, 32)));
            let _ = headers.insert(random::string(g, 32), Some(random::string(g, 32)));

            let properties = None;
            let request_id = Uuid::new_v4();

            UpdateObjectPayload {
                owner,
                bucket_id,
                name,
                id,
                vnode,
                content_type,
                headers,
                properties,
                request_id,
            }
        }
    }

    quickcheck! {
        fn prop_update_object_payload_roundtrip(msg: UpdateObjectPayload) -> bool {
            match serde_json::to_string(&msg) {
                Ok(update_str) => {
                    let decode_result: Result<UpdateObjectPayload, _> =
                        serde_json::from_str(&update_str);
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
        fn prop_updateobject_payload_from_json(json: UpdateObjectJson) -> bool {
            let decode_result1: Result<UpdateObjectPayload, _> =
                serde_json::from_value(json.0.clone());
            let res1 = decode_result1.is_ok();

            let decode_result2: Result<Vec<UpdateObjectPayload>, _> =
                serde_json::from_value(Value::Array(vec![json.0]));
            let res2 = decode_result2.is_ok();

            res1 && res2
        }
    }
}
