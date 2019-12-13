// Copyright 2019 Joyent, Inc.

use serde_derive::{Deserialize, Serialize};
use serde_json::Error as SerdeError;
use serde_json::{json, Value};
use slog::{debug, error, Logger};

use cueball_postgres_connection::PostgresConnection;
use rust_fast::protocol::{FastMessage, FastMessageData};
use uuid::Uuid;

use crate::gc;
use crate::object::ObjectResponse;
use crate::sql;
use crate::types::{HandlerResponse, HasRequestId, RowSlice};
use crate::util::array_wrap;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct GetGarbagePayload {
    pub request_id: Uuid,
}

impl HasRequestId for GetGarbagePayload {
    fn request_id(&self) -> Uuid {
        self.request_id
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct GetGarbageResponse {
    pub batch_id: Option<Uuid>,
    pub garbage: Vec<ObjectResponse>,
}

pub(self) fn to_json(gr: GetGarbageResponse) -> Value {
    // This conversion can fail if the implementation of Serialize decides to
    // fail, or if the type contains a map with non-string keys. There is no
    // reason for the former to occur and we have JSON roundtrip quickcheck
    // testing to verify this. The ObjectResponse type does not contain any maps
    // so the latter reason for failure is not a concern either.
    serde_json::to_value(gr).expect("failed to serialize GetGarbageResponse")
}

pub(crate) fn decode_msg(
    value: &Value,
) -> Result<Vec<GetGarbagePayload>, SerdeError> {
    serde_json::from_value::<Vec<GetGarbagePayload>>(value.clone())
}

#[allow(clippy::needless_pass_by_value)]
pub(crate) fn action(
    msg_id: u32,
    method: &str,
    log: &Logger,
    _payload: GetGarbagePayload,
    conn: &mut PostgresConnection,
) -> Result<HandlerResponse, String> {
    // Make database request
    do_get(method, conn, log)
        .and_then(|resp| {
            // Handle the successful database response
            debug!(log, "{} operation was successful", &method);

            let value = to_json(resp);
            let msg_data =
                FastMessageData::new(method.into(), array_wrap(value));
            let msg: HandlerResponse =
                FastMessage::data(msg_id, msg_data).into();
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
            let msg: HandlerResponse =
                FastMessage::data(msg_id, msg_data).into();
            Ok(msg)
        })
}

fn do_get(
    method: &str,
    mut conn: &mut PostgresConnection,
    log: &Logger,
) -> Result<GetGarbageResponse, String> {
    let sql = get_sql();

    sql::query(sql::Method::GarbageGet, &mut conn, sql, &[], log)
        .map_err(|e| e.to_string())
        .and_then(|rows| {
            if rows.is_empty() {
                // Try to refresh the garbage view in case the view is stale
                sql::execute(
                    sql::Method::GarbageRefresh,
                    &mut conn,
                    gc::refresh_garbage_view_sql(),
                    &[],
                    log,
                )
                .and_then(|_| {
                    sql::query(
                        sql::Method::GarbageGet,
                        &mut conn,
                        sql,
                        &[],
                        log,
                    )
                })
                .map_err(|e| e.to_string())
            } else {
                Ok(rows)
            }
        })
        .and_then(|rows| response(method, &rows))
}

fn get_sql() -> &'static str {
    "SELECT * FROM GARBAGE_BATCH"
}

pub(self) fn response(
    _method: &str,
    rows: &RowSlice,
) -> Result<GetGarbageResponse, String> {
    let mut garbage: Vec<ObjectResponse> = Vec::with_capacity(1024);
    for row in rows {
        let content_md5_bytes: Vec<u8> = row.get("content_md5");
        let content_md5 = base64::encode(&content_md5_bytes);
        let garbage_item = ObjectResponse {
            id: row.get("id"),
            owner: row.get("owner"),
            bucket_id: row.get("bucket_id"),
            name: row.get("name"),
            created: row.get("created"),
            modified: row.get("modified"),
            content_length: row.get("content_length"),
            content_md5,
            content_type: row.get("content_type"),
            headers: row.get("headers"),
            sharks: row.get("sharks"),
            properties: row.get("properties"),
        };
        garbage.push(garbage_item);
    }

    let batch_id = if rows.is_empty() {
        Some(Uuid::new_v4())
    } else {
        None
    };

    Ok(GetGarbageResponse { batch_id, garbage })
}

#[cfg(test)]
mod test {
    use super::*;

    use quickcheck::{quickcheck, Arbitrary, Gen};
    use serde_json;
    use serde_json::Map;

    #[derive(Clone, Debug)]
    struct GetGarbageJson(Value);

    impl Arbitrary for GetGarbageJson {
        fn arbitrary<G: Gen>(_g: &mut G) -> Self {
            let request_id = serde_json::to_value(Uuid::new_v4())
                .expect("failed to convert request_id field to Value");

            let mut obj = Map::new();
            obj.insert("request_id".into(), request_id);
            GetGarbageJson(Value::Object(obj))
        }
    }

    impl Arbitrary for GetGarbagePayload {
        fn arbitrary<G: Gen>(_g: &mut G) -> Self {
            let request_id = Uuid::new_v4();

            GetGarbagePayload { request_id }
        }
    }

    impl Arbitrary for GetGarbageResponse {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let garbage = Vec::<ObjectResponse>::arbitrary(g);
            let batch_id = if garbage.is_empty() {
                None
            } else {
                Some(Uuid::new_v4())
            };

            GetGarbageResponse { batch_id, garbage }
        }
    }

    quickcheck! {
        fn prop_get_garbage_payload_roundtrip(msg: GetGarbagePayload) -> bool {
            match serde_json::to_string(&msg) {
                Ok(get_str) => {
                    let decode_result: Result<GetGarbagePayload, _> =
                        serde_json::from_str(&get_str);
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
        fn prop_get_garbage_payload_from_json(json: GetGarbageJson) -> bool {
            let decode_result1: Result<GetGarbagePayload, _> =
                serde_json::from_value(json.0.clone());
            let res1 = decode_result1.is_ok();

            let decode_result2: Result<Vec<GetGarbagePayload>, _> =
                serde_json::from_value(Value::Array(vec![json.0]));
            let res2 = decode_result2.is_ok();

            res1 && res2
        }
    }

    quickcheck! {
        fn prop_get_garbage_response_to_json(objr: GetGarbageResponse) -> bool {
            // Test the conversion to JSON. A lack of a panic in the call the
            // `to_json` indicates success.
            let _ = to_json(objr);
            true
        }
    }
}
