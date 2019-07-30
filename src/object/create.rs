// Copyright 2019 Joyent, Inc.

use std::vec::Vec;

use base64;
use serde_derive::{Deserialize, Serialize};
use serde_json::{Value, json};
use slog::{Logger, debug, error, warn};
use uuid::Uuid;

use cueball_postgres_connection::PostgresConnection;
use rust_fast::protocol::{FastMessage, FastMessageData};

use crate::object::{
    ObjectResponse,
    StorageNodeIdentifier,
    insert_delete_table_sql,
    response,
    to_json
};
use crate::sql;
use crate::util::{
    HandlerError,
    HandlerResponse,
    Hstore,
    array_wrap,
    other_error
};

const METHOD: &str = "createobject";

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CreateObjectPayload {
    pub owner          : Uuid,
    pub bucket_id      : Uuid,
    pub name           : String,
    pub id             : Uuid,
    pub vnode          : u64,
    pub content_length : i64,
    pub content_md5    : String,
    pub content_type   : String,
    pub headers        : Hstore,
    pub sharks         : Vec<StorageNodeIdentifier>,
    pub properties     : Option<Value>,
    pub request_id     : Uuid
}

pub(crate) fn handler(
    msg_id: u32,
    data: &Value,
    mut conn: &mut PostgresConnection,
    log: &Logger
) -> Result<HandlerResponse, HandlerError>
{
    debug!(log, "handling {} function request", &METHOD);

    serde_json::from_value::<Vec<CreateObjectPayload>>(data.clone())
        .map_err(|e| e.to_string())
        .and_then(|mut arr| {
            // Remove outer JSON array required by Fast
            if !arr.is_empty() {
                Ok(arr.remove(0))
            } else {
                let err_msg = "Failed to parse JSON data as payload for \
                               createobject function";
                warn!(log, "{}: {}", err_msg, data);
                Err(err_msg.to_string())
            }
        })
        .and_then(|payload| {
            // Make database request
            let req_id = payload.request_id;
            debug!(log, "parsed CreateObjectPayload, req_id: {}", &req_id);

            create(payload, &mut conn)
                .and_then(|maybe_resp| {
                    // Handle the successful database response
                    debug!(log, "{} operation was successful, req_id: {}", &METHOD, &req_id);
                    // The `None` branch of the following match statement should
                    // never be reached. If `maybe_resp` was `None` this would
                    // mean that the SQL INSERT for the object was successful
                    // and the transaction committed, but no results were
                    // returned from the RETURNING clause. This should not be
                    // possible, but for completeleness we include a check for
                    // the condition.
                    let value =
                        match maybe_resp {
                            Some(resp) => to_json(resp),
                            None => object_create_failed()
                        };
                    let msg_data =
                        FastMessageData::new(METHOD.into(), array_wrap(value));
                    let msg: HandlerResponse =
                        FastMessage::data(msg_id, msg_data).into();
                    Ok(msg)
                })
                .or_else(|e| {
                    // Handle database error response
                    error!(log, "{} operation failed: {}, req_id: {}", &METHOD, &e, &req_id);

                    // Database errors are returned to as regular Fast messages
                    // to be handled by the calling application
                    let value = array_wrap(json!({
                        "name": "PostgresError",
                        "message": e
                    }));

                    let msg_data = FastMessageData::new(METHOD.into(), value);
                    let msg: HandlerResponse =
                        FastMessage::data(msg_id, msg_data).into();
                    Ok(msg)
                })
        })
        .map_err(|e| HandlerError::IO(other_error(&e)))
}

fn create(
    payload: CreateObjectPayload,
    conn: &mut PostgresConnection
) -> Result<Option<ObjectResponse>, String>
{
    let mut txn = (*conn).transaction().map_err(|e| e.to_string())?;
    let create_sql = create_sql(payload.vnode);
    let move_sql = insert_delete_table_sql(payload.vnode);
    let content_md5_bytes =
        base64::decode(&payload.content_md5)
        .map_err(|e| format!("content_md5 is not valid base64 encoded data: {}",
                             e.to_string()))?;

    sql::txn_execute(sql::Method::ObjectCreateMove, &mut txn, move_sql.as_str(),
                     &[&payload.owner,
                       &payload.bucket_id,
                       &payload.name])
        .and_then(|_moved_rows| {
            sql::txn_query(sql::Method::ObjectCreate, &mut txn, create_sql.as_str(),
                           &[&payload.id,
                           &payload.owner,
                           &payload.bucket_id,
                           &payload.name,
                           &payload.content_length,
                           &content_md5_bytes,
                           &payload.content_type,
                           &payload.headers,
                           &payload.sharks,
                           &payload.properties])
        })
        .and_then(|rows| {
            txn.commit()?;
            Ok(rows)
        })
        .map_err(|e| e.to_string())
        .and_then(|rows| {
            response(METHOD, rows)
        })
}

fn create_sql(
    vnode: u64
) -> String
{
    ["INSERT INTO manta_bucket_",
     &vnode.to_string(),
     &".manta_bucket_object ( \
       id, owner, bucket_id, name, content_length, content_md5, \
       content_type, headers, sharks, properties) \
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
       ON CONFLICT (owner, bucket_id, name) DO UPDATE \
       SET id = EXCLUDED.id, \
       created = current_timestamp, \
       modified = current_timestamp, \
       content_length = EXCLUDED.content_length, \
       content_md5 = EXCLUDED.content_md5, \
       content_type = EXCLUDED.content_type, \
       headers = EXCLUDED.headers, \
       sharks = EXCLUDED.sharks, \
       properties = EXCLUDED.properties \
       RETURNING id, owner, bucket_id, name, created, modified, \
       content_length, content_md5, content_type, headers, \
       sharks, properties"].concat()
}

// This error is only here for completeness. In practice it should never
// actually be called. See the invocation in this module for more information.
fn object_create_failed() -> Value {
    json!({
        "name": "PostgresError",
        "message": "Create statement failed to return any results"
    })
}


#[cfg(test)]
mod test {
    use super::*;

    use std::collections::HashMap;

    use quickcheck::{quickcheck, Arbitrary, Gen};
    use quickcheck_helpers::random;
    use serde_json;
    use serde_json::Map;

    use crate::object;

    #[derive(Clone, Debug)]
    struct CreateObjectJson(Value);

    impl Arbitrary for CreateObjectJson {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let owner = serde_json::to_value(Uuid::new_v4())
                .expect("failed to convert owner field to Value");
            let name = serde_json::to_value(random::string(g, 63))
                .expect("failed to convert name field to Value");
            let bucket_id = serde_json::to_value(Uuid::new_v4())
                .expect("failed to convert bucket_id field to Value");
            let id = serde_json::to_value(Uuid::new_v4())
                .expect("failed to convert id field to Value");
            let vnode = serde_json::to_value(u64::arbitrary(g))
                .expect("failed to convert vnode field to Value");
            let content_length = serde_json::to_value(i64::arbitrary(g))
                .expect("failed to convert content_length field to Value");
            let content_md5 = serde_json::to_value(random::string(g, 20))
                .expect("failed to convert content_md5 field to Value");
            let content_type = serde_json::to_value(random::string(g, 32))
                .expect("failed to convert content_type field to Value");
            let mut headers = HashMap::new();
            let _ = headers.insert(
                random::string(g, 32),
                Some(random::string(g, 32))
            );
            let _ = headers.insert(
                random::string(g, 32),
                Some(random::string(g, 32))
            );
            let headers = serde_json::to_value(headers)
                .expect("failed to convert headers field to Value");
            let shark1 = object::StorageNodeIdentifier {
                datacenter: random::string(g, 32),
                manta_storage_id: random::string(g, 32)
            };
            let shark2 = object::StorageNodeIdentifier {
                datacenter: random::string(g, 32),
                manta_storage_id: random::string(g, 32)
            };
            let sharks = serde_json::to_value(vec![shark1, shark2])
                .expect("failed to convert sharks field to Value");
            let request_id = serde_json::to_value(Uuid::new_v4())
                .expect("failed to convert request_id field to Value");

            let mut obj = Map::new();
            obj.insert("owner".into(), owner);
            obj.insert("name".into(), name);
            obj.insert("bucket_id".into(), bucket_id);
            obj.insert("id".into(), id);
            obj.insert("vnode".into(), vnode);
            obj.insert("content_length".into(), content_length);
            obj.insert("content_md5".into(), content_md5);
            obj.insert("content_type".into(), content_type);
            obj.insert("headers".into(), headers);
            obj.insert("sharks".into(), sharks);
            obj.insert("request_id".into(), request_id);
            CreateObjectJson(Value::Object(obj))
        }
    }

    impl Arbitrary for CreateObjectPayload {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let owner = Uuid::new_v4();
            let bucket_id = Uuid::new_v4();
            let name = random::string(g, 32);
            let id = Uuid::new_v4();
            let vnode = u64::arbitrary(g);
            let content_length = i64::arbitrary(g);
            let content_type = random::string(g, 32);
            let content_md5 = random::string(g, 32);
            let mut headers = HashMap::new();
            let _ = headers.insert(
                random::string(g, 32),
                Some(random::string(g, 32))
            );
            let _ = headers.insert(
                random::string(g, 32),
                Some(random::string(g, 32))
            );

            let shark1 = StorageNodeIdentifier {
                datacenter: random::string(g, 32),
                manta_storage_id: random::string(g, 32)
            };
            let shark2 = StorageNodeIdentifier {
                datacenter: random::string(g, 32),
                manta_storage_id: random::string(g, 32)
            };
            let sharks = vec![shark1, shark2];
            let properties = None;
            let request_id = Uuid::new_v4();

            CreateObjectPayload {
                owner,
                bucket_id,
                name,
                id,
                vnode,
                content_length,
                content_md5,
                content_type,
                headers,
                sharks,
                properties,
                request_id
            }
        }
    }

    quickcheck! {
        fn prop_create_object_payload_roundtrip(msg: CreateObjectPayload) -> bool {
            match serde_json::to_string(&msg) {
                Ok(create_str) => {
                    let decode_result: Result<CreateObjectPayload, _> =
                        serde_json::from_str(&create_str);
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
        fn prop_createobject_payload_from_json(json: CreateObjectJson) -> bool {
            let decode_result1: Result<CreateObjectPayload, _> =
                serde_json::from_value(json.0.clone());
            let res1 = decode_result1.is_ok();

            let decode_result2: Result<Vec<CreateObjectPayload>, _> =
                serde_json::from_value(Value::Array(vec![json.0]));
            let res2 = decode_result2.is_ok();

            res1 && res2
        }
    }
}
