// Copyright 2020 Joyent, Inc.

use std::collections::HashMap;
use std::vec::Vec;

use base64;
use crossbeam_channel::Sender;
use rocksdb::Error as RocksdbError;
use serde_derive::{Deserialize, Serialize};
use serde_json::Error as SerdeError;
use serde_json::Value;
use slog::{debug, error, Logger};
use uuid::Uuid;

use cueball_postgres_connection::PostgresConnection;
use fast_rpc::protocol::{FastMessage, FastMessageData};

use crate::conditional;
use crate::error::BucketsMdapiError;
use crate::metrics::RegisteredMetrics;
use crate::object::{
    insert_delete_table_sql, response, to_json, ObjectResponse,
    StorageNodeIdentifier,
};
use crate::sql;
use crate::types::{HandlerResponse, HasRequestId, Hstore, KVOps};
use crate::util::array_wrap;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CreateObjectPayload {
    pub owner: Uuid,
    pub bucket_id: Uuid,
    pub name: String,
    pub id: Uuid,
    pub vnode: u64,
    pub content_length: i64,
    pub content_md5: String,
    pub content_type: String,
    pub headers: Hstore,
    pub sharks: Vec<StorageNodeIdentifier>,
    pub properties: Option<Value>,
    pub request_id: Uuid,

    #[serde(default)]
    pub conditions: conditional::Conditions,
}

impl HasRequestId for CreateObjectPayload {
    fn request_id(&self) -> Uuid {
        self.request_id
    }
}

pub(crate) fn decode_msg(
    value: &Value,
) -> Result<Vec<CreateObjectPayload>, SerdeError> {
    serde_json::from_value::<Vec<CreateObjectPayload>>(value.clone())
}

#[allow(clippy::needless_pass_by_value)]
pub(crate) fn action(
    msg_id: u32,
    method: &str,
    metrics: &RegisteredMetrics,
    log: &Logger,
    payload: CreateObjectPayload,
    send_channel_map: &HashMap<
        u64,
        Sender<(
            KVOps,
            Vec<u8>,
            Option<Vec<u8>>,
            Sender<Result<Option<Vec<u8>>, RocksdbError>>,
        )>,
    >,
) -> Result<HandlerResponse, String> {
    // Make database request
    do_create(method, &payload, send_channel_map, metrics, log)
        .and_then(|maybe_resp| {
            // Handle the successful database response
            debug!(log, "operation successful");
            // The `None` branch of the following match statement should
            // never be reached. If `maybe_resp` was `None` this would
            // mean that the SQL INSERT for the object was successful
            // and the transaction committed, but no results were
            // returned from the RETURNING clause. This should not be
            // possible, but for completeleness we include a check for
            // the condition.
            let value = match maybe_resp {
                Some(resp) => to_json(resp),
                None => object_create_failed(),
            };
            let msg_data =
                FastMessageData::new(method.into(), array_wrap(value));
            let msg: HandlerResponse =
                FastMessage::data(msg_id, msg_data).into();
            Ok(msg)
        })
        .or_else(|e| {
            if let BucketsMdapiError::PostgresError(_) = &e {
                error!(log, "operation failed"; "error" => e.message());
            }

            let msg_data =
                FastMessageData::new(method.into(), array_wrap(e.into_fast()));
            let msg: HandlerResponse =
                FastMessage::data(msg_id, msg_data).into();
            Ok(msg)
        })
}

fn do_create(
    method: &str,
    payload: &CreateObjectPayload,
    send_channel_map: &HashMap<
        u64,
        Sender<(
            KVOps,
            Vec<u8>,
            Option<Vec<u8>>,
            Sender<Result<Option<Vec<u8>>, RocksdbError>>,
        )>,
    >,
    metrics: &RegisteredMetrics,
    log: &Logger,
) -> Result<Option<ObjectResponse>, BucketsMdapiError> {
    // let mut txn = (*conn)
    //     .transaction()
    //     .map_err(|e| BucketsMdapiError::PostgresError(e.to_string()))?;
    // let create_sql = create_sql(payload.vnode);
    // let move_sql = insert_delete_table_sql(payload.vnode);
    // let content_md5_bytes = base64::decode(&payload.content_md5)
    //     .map_err(|e| BucketsMdapiError::ContentMd5Error(e.to_string()))?;

    // conditional::request(
    //     &mut txn,
    //     &[&payload.owner, &payload.bucket_id, &payload.name],
    //     payload.vnode,
    //     &payload.conditions,
    //     metrics,
    //     log,
    // )
    // .and_then(|_| {
    //     sql::txn_execute(
    //         sql::Method::ObjectCreateMove,
    //         &mut txn,
    //         move_sql.as_str(),
    //         &[&payload.owner, &payload.bucket_id, &payload.name],
    //         metrics,
    //         log,
    //     )
    //     .and_then(|_moved_rows| {
    //         sql::txn_query(
    //             sql::Method::ObjectCreate,
    //             &mut txn,
    //             create_sql.as_str(),
    //             &[
    //                 &payload.id,
    //                 &payload.owner,
    //                 &payload.bucket_id,
    //                 &payload.name,
    //                 &payload.content_length,
    //                 &content_md5_bytes,
    //                 &payload.content_type,
    //                 &payload.headers,
    //                 &payload.sharks,
    //                 &payload.properties,
    //             ],
    //             metrics,
    //             log,
    //         )
    //     })
    //     .map_err(|e| BucketsMdapiError::PostgresError(e.to_string()))
    // })
    // .and_then(|rows| {
    //     txn.commit()
    //         .map_err(|e| BucketsMdapiError::PostgresError(e.to_string()))?;
    //     Ok(rows)
    // })
    // .and_then(|rows| response(method, &rows))
    std::unimplemented!()
}

fn create_sql(vnode: u64) -> String {
    [
        "INSERT INTO manta_bucket_",
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
          sharks, properties",
    ]
    .concat()
}

// This error is only here for completeness. In practice it should never
// actually be called. See the invocation in this module for more information.
fn object_create_failed() -> Value {
    serde_json::to_value(BucketsMdapiError::PostgresError(
        "Create statement failed to return any results".to_string(),
    ))
    .expect("failed to encode a PostgresError error")
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
            let _ = headers
                .insert(random::string(g, 32), Some(random::string(g, 32)));
            let _ = headers
                .insert(random::string(g, 32), Some(random::string(g, 32)));
            let headers = serde_json::to_value(headers)
                .expect("failed to convert headers field to Value");
            let shark_1 = object::StorageNodeIdentifier {
                datacenter: random::string(g, 32),
                manta_storage_id: random::string(g, 32),
            };
            let shark_2 = object::StorageNodeIdentifier {
                datacenter: random::string(g, 32),
                manta_storage_id: random::string(g, 32),
            };
            let sharks = serde_json::to_value(vec![shark_1, shark_2])
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
            let _ = headers
                .insert(random::string(g, 32), Some(random::string(g, 32)));
            let _ = headers
                .insert(random::string(g, 32), Some(random::string(g, 32)));

            let shark_1 = StorageNodeIdentifier {
                datacenter: random::string(g, 32),
                manta_storage_id: random::string(g, 32),
            };
            let shark_2 = StorageNodeIdentifier {
                datacenter: random::string(g, 32),
                manta_storage_id: random::string(g, 32),
            };
            let sharks = vec![shark_1, shark_2];
            let properties = None;
            let request_id = Uuid::new_v4();
            let conditions: conditional::Conditions = Default::default();

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
                request_id,
                conditions,
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
