// Copyright 2020 Joyent, Inc.

use chrono;
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::error::BucketsMdapiError;
use crate::types::{HasRequestId, RowSlice};

pub mod create;
pub mod delete;
pub mod get;
pub mod list;

type Timestamptz = chrono::DateTime<chrono::Utc>;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct GetBucketPayload {
    pub owner: Uuid,
    pub name: String,
    pub vnode: u64,
    pub request_id: Uuid,
}

impl HasRequestId for GetBucketPayload {
    fn request_id(&self) -> Uuid {
        self.request_id
    }
}

pub type DeleteBucketPayload = GetBucketPayload;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct BucketResponse {
    pub id: Uuid,
    pub owner: Uuid,
    pub name: String,
    pub created: Timestamptz,
}

pub(self) fn to_json(br: BucketResponse) -> Value {
    // This conversion can fail if the implementation of Serialize decides to
    // fail, or if the type contains a map with non-string keys. There is no
    // reason for the former to occur and we have JSON roundtrip quickcheck
    // testing to verify this. The BucketResponse type does not contain any maps
    // so the latter reason for failure is not a concern either.
    serde_json::to_value(br).expect("failed to serialize BucketResponse")
}

pub(self) fn bucket_not_found() -> Value {
    BucketsMdapiError::BucketNotFound.into_fast()
}

pub(self) fn bucket_already_exists() -> Value {
    BucketsMdapiError::BucketAlreadyExists.into_fast()
}

pub(self) fn response(
    method: &str,
    rows: &RowSlice,
) -> Result<Option<BucketResponse>, String> {
    if rows.is_empty() {
        Ok(None)
    } else if rows.len() == 1 {
        let row = &rows[0];
        let cols = row.len();

        if cols >= 4 {
            let resp = BucketResponse {
                id: row.get("id"),
                owner: row.get("owner"),
                name: row.get("name"),
                created: row.get("created"),
            };
            Ok(Some(resp))
        } else {
            let err = format!(
                "{} query returned a row with only {} columns, \
                 but 4 were expected .",
                method, cols
            );
            Err(err)
        }
    } else {
        // The schema should prevent there ever being multiple rows in the query result
        let err = format!(
            "{} query found {} results, but expected only 1.",
            method,
            rows.len()
        );
        Err(err)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use chrono::prelude::*;
    use quickcheck::{quickcheck, Arbitrary, Gen};
    use quickcheck_helpers::random;
    use serde_json;
    use serde_json::Map;

    #[derive(Clone, Debug)]
    struct GetBucketJson(Value);
    #[derive(Clone, Debug)]
    struct BucketJson(Value);

    impl Arbitrary for GetBucketJson {
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
            GetBucketJson(Value::Object(obj))
        }
    }

    impl Arbitrary for GetBucketPayload {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let owner = Uuid::new_v4();
            let name = random::string(g, 32);
            let vnode = u64::arbitrary(g);
            let request_id = Uuid::new_v4();

            GetBucketPayload {
                owner,
                name,
                vnode,
                request_id,
            }
        }
    }

    impl Arbitrary for BucketResponse {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let id = Uuid::new_v4();
            let owner = Uuid::new_v4();
            let name = random::string(g, 32);
            let created = Utc::now();

            BucketResponse {
                id,
                owner,
                name,
                created,
            }
        }
    }

    quickcheck! {
        fn prop_get_bucket_payload_roundtrip(msg: GetBucketPayload) -> bool {
            match serde_json::to_string(&msg) {
                Ok(create_str) => {
                    let decode_result: Result<GetBucketPayload, _> =
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
        fn prop_bucket_response_roundtrip(msg: BucketResponse) -> bool {
            match serde_json::to_string(&msg) {
                Ok(response_str) => {
                    let decode_result: Result<BucketResponse, _> =
                        serde_json::from_str(&response_str);
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
        fn prop_getbucket_payload_from_json(json: GetBucketJson) -> bool {
            let decode_result1: Result<GetBucketPayload, _> =
                serde_json::from_value(json.0.clone());
            let res1 = decode_result1.is_ok();

            let decode_result2: Result<Vec<GetBucketPayload>, _> =
                serde_json::from_value(Value::Array(vec![json.0]));
            let res2 = decode_result2.is_ok();

            res1 && res2
        }
    }

    quickcheck! {
        fn prop_bucket_response_to_json(br: BucketResponse) -> bool {
            // Test the conversion to JSON. A lack of a panic in the call the
            // `to_json` indicates success.
            let _ = to_json(br);
            true
        }
    }
}
