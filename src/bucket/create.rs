// Copyright 2019 Joyent, Inc.

use serde_derive::{Deserialize, Serialize};
use serde_json::Error as SerdeError;
use serde_json::{json, Value};
use slog::{debug, error, Logger};
use uuid::Uuid;

use cueball_postgres_connection::PostgresConnection;
use rust_fast::protocol::{FastMessage, FastMessageData};

use crate::bucket::{bucket_already_exists, response, to_json, BucketResponse};
use crate::sql;
use crate::types::{HandlerResponse, HasRequestId};
use crate::util::array_wrap;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CreateBucketPayload {
    pub owner: Uuid,
    pub name: String,
    pub vnode: u64,
    pub request_id: Uuid,
}

impl HasRequestId for CreateBucketPayload {
    fn request_id(&self) -> Uuid {
        self.request_id
    }
}

pub(crate) fn decode_msg(value: &Value) -> Result<Vec<CreateBucketPayload>, SerdeError> {
    serde_json::from_value::<Vec<CreateBucketPayload>>(value.clone())
}

pub(crate) fn action(
    msg_id: u32,
    method: &str,
    log: &Logger,
    payload: CreateBucketPayload,
    conn: &mut PostgresConnection,
) -> Result<HandlerResponse, String> {
    // Make database request
    do_create(method, payload, conn)
        .and_then(|maybe_resp| {
            // Handle the successful database response
            debug!(log, "{} operation was successful", &method);
            let value = match maybe_resp {
                Some(resp) => to_json(resp),
                None => bucket_already_exists(),
            };
            let msg_data = FastMessageData::new(method.into(), array_wrap(value));
            let msg: HandlerResponse = FastMessage::data(msg_id, msg_data).into();
            Ok(msg)
        })
        .or_else(|e| {
            // Handle database error response
            error!(log, "{} operation failed: {}", &method, &e);

            // Database errors are returned to as regular Fast messages
            // to be handled by the calling application
            let value = array_wrap(json!({
                "name": "PostgresError",
                "message": e
            }));

            let msg_data = FastMessageData::new(method.into(), value);
            let msg: HandlerResponse = FastMessage::data(msg_id, msg_data).into();
            Ok(msg)
        })
}

fn do_create(
    method: &str,
    payload: CreateBucketPayload,
    conn: &mut PostgresConnection,
) -> Result<Option<BucketResponse>, String> {
    let mut txn = (*conn).transaction().map_err(|e| e.to_string())?;
    let create_sql = create_sql(payload.vnode);

    sql::txn_query(
        sql::Method::BucketCreate,
        &mut txn,
        create_sql.as_str(),
        &[&Uuid::new_v4(), &payload.owner, &payload.name],
    )
    .and_then(|rows| {
        txn.commit()?;
        Ok(rows)
    })
    .map_err(|e| e.to_string())
    .and_then(|rows| response(method, rows))
}

fn create_sql(vnode: u64) -> String {
    [
        "INSERT INTO manta_bucket_",
        &vnode.to_string(),
        &".manta_bucket \
          (id, owner, name) \
          VALUES ($1, $2, $3) \
          ON CONFLICT DO NOTHING \
          RETURNING id, owner, name, created",
    ]
    .concat()
}

#[cfg(test)]
mod test {
    use super::*;

    use quickcheck::{quickcheck, Arbitrary, Gen};
    use quickcheck_helpers::random;
    use serde_json;
    use serde_json::Map;

    #[derive(Clone, Debug)]
    struct CreateBucketJson(Value);

    impl Arbitrary for CreateBucketJson {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let owner = serde_json::to_value(Uuid::new_v4())
                .expect("failed to convert owner field to Value");
            let name = serde_json::to_value(random::string(g, 63))
                .expect("failed to convert name field to Value");
            let vnode = serde_json::to_value(u64::arbitrary(g))
                .expect("failed to convert vnode field to Value");
            let request_id = serde_json::to_value(Uuid::new_v4())
                .expect("failed to convert request_id field to Value");

            let mut obj = Map::new();
            obj.insert("owner".into(), owner);
            obj.insert("name".into(), name);
            obj.insert("vnode".into(), vnode);
            obj.insert("request_id".into(), request_id);
            CreateBucketJson(Value::Object(obj))
        }
    }

    impl Arbitrary for CreateBucketPayload {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let owner = Uuid::new_v4();
            let name = random::string(g, 32);
            let vnode = u64::arbitrary(g);
            let request_id = Uuid::new_v4();

            CreateBucketPayload {
                owner,
                name,
                vnode,
                request_id,
            }
        }
    }

    quickcheck! {
        fn prop_create_bucket_payload_roundtrip(msg: CreateBucketPayload) -> bool {
            match serde_json::to_string(&msg) {
                Ok(create_str) => {
                    let decode_result: Result<CreateBucketPayload, _> =
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
        fn prop_createbucket_payload_from_json(json: CreateBucketJson) -> bool {
            let decode_result1: Result<CreateBucketPayload, _> =
                serde_json::from_value(json.0.clone());
            let res1 = decode_result1.is_ok();

            let decode_result2: Result<Vec<CreateBucketPayload>, _> =
                serde_json::from_value(Value::Array(vec![json.0]));
            let res2 = decode_result2.is_ok();

            res1 && res2
        }
    }
}
