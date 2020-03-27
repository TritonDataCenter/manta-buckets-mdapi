// Copyright 2020 Joyent, Inc.

use std::error::Error;
use std::vec::Vec;

use base64;
use postgres::types::{FromSql, IsNull, ToSql, Type};
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;
use tokio_postgres::{accepts, to_sql_checked};
use uuid::Uuid;

use crate::error::BucketsMdapiError;
use crate::types::{HasRequestId, Hstore, RowSlice, Timestamptz};
use crate::conditional;

pub mod create;
pub mod delete;
pub mod get;
pub mod list;
pub mod update;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct GetObjectPayload {
    pub owner: Uuid,
    pub bucket_id: Uuid,
    pub name: String,
    pub vnode: u64,
    pub request_id: Uuid,

    #[serde(default)]
    pub conditions: conditional::Conditions,
}

impl HasRequestId for GetObjectPayload {
    fn request_id(&self) -> Uuid {
        self.request_id
    }
}

type DeleteObjectPayload = GetObjectPayload;

/// A type that represents the information about the datacenter and storage node
/// id of a copy of an object's data.
///
/// The incoming representation of the sharks data is a JSON array of objects
/// where each object has two keys: `datacenter` and `manta_storage_id`. The custom
/// `ToSQL` instance for the this type converts the each object in this
/// representation into a String that represents the same data in fewer
/// bytes. The postgres column type for the sharks column is a text array.
///
/// Likewise, the custom `FromSql` instance for the type converts the members of
/// the text array format stored in the database back into an instance of
/// `StorageNodeIdentifier`.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct StorageNodeIdentifier {
    pub datacenter: String,
    pub manta_storage_id: String,
}

impl ToString for StorageNodeIdentifier {
    fn to_string(&self) -> String {
        [&self.datacenter, ":", &self.manta_storage_id].concat()
    }
}

impl From<String> for StorageNodeIdentifier {
    fn from(s: String) -> Self {
        let v: Vec<&str> = s.split(':').collect();
        Self {
            datacenter: String::from(v[0]),
            manta_storage_id: String::from(v[1]),
        }
    }
}

impl ToSql for StorageNodeIdentifier {
    fn to_sql(
        &self,
        ty: &Type,
        w: &mut Vec<u8>,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        <String as ToSql>::to_sql(&self.to_string(), ty, w)
    }

    accepts!(TEXT);

    to_sql_checked!();
}

impl<'a> FromSql<'a> for StorageNodeIdentifier {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        String::from_sql(ty, raw).and_then(|s| Ok(Self::from(s)))
    }

    accepts!(TEXT);
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct DeleteObjectResponse {
    pub id: Uuid,
    pub owner: Uuid,
    pub bucket_id: Uuid,
    pub name: String,
    pub content_length: i64,
    pub shark_count: i32,
}

impl DeleteObjectResponse {
    pub fn to_json(&self) -> Value {
        // Similar to ObjectResponse, the serialization to JSON might fail if the
        // implementation of Serialize decideds to fail. We have JSON roundtrip
        // quickcheck testing to verify we can serialize and deserialize the same
        // object safely and correctly.
        serde_json::to_value(self)
            .expect("failed to serialize DeleteObjectResponse")
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ObjectResponse {
    pub id: Uuid,
    pub owner: Uuid,
    pub bucket_id: Uuid,
    pub name: String,
    pub created: Timestamptz,
    pub modified: Timestamptz,
    pub content_length: i64,
    pub content_md5: String,
    pub content_type: String,
    pub headers: Hstore,
    pub sharks: Vec<StorageNodeIdentifier>,
    pub properties: Option<Value>,
}

pub(self) fn to_json(objr: ObjectResponse) -> Value {
    // This conversion can fail if the implementation of Serialize decides to
    // fail, or if the type contains a map with non-string keys. There is no
    // reason for the former to occur and we have JSON roundtrip quickcheck
    // testing to verify this. The ObjectResponse type does not contain any maps
    // so the latter reason for failure is not a concern either.
    serde_json::to_value(objr).expect("failed to serialize ObjectResponse")
}

pub fn object_not_found() -> Value {
    BucketsMdapiError::ObjectNotFound.into_fast()
}

pub(self) fn response(
    method: &str,
    rows: &RowSlice,
) -> Result<Option<ObjectResponse>, BucketsMdapiError> {
    if rows.is_empty() {
        Ok(None)
    } else if rows.len() == 1 {
        let row = &rows[0];
        let cols = row.len();

        if cols >= 12 {
            let content_md5_bytes: Vec<u8> = row.get("content_md5");
            let content_md5 = base64::encode(&content_md5_bytes);
            let resp = ObjectResponse {
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
            Ok(Some(resp))
        } else {
            let err = format!(
                "{} query returned a row with only {} columns, \
                 but 12 were expected.",
                method, cols
            );
            Err(BucketsMdapiError::PostgresError(err.to_string()))
        }
    } else {
        let err = format!(
            "{} query found {} results, but expected only 1.",
            method,
            rows.len()
        );
        Err(BucketsMdapiError::PostgresError(err.to_string()))
    }
}

pub(self) fn insert_delete_table_sql(vnode: u64) -> String {
    let vnode_str = vnode.to_string();
    [
        "INSERT INTO manta_bucket_",
        &vnode_str,
        &".manta_bucket_deleted_object ( \
          id, owner, bucket_id, name, created, modified, \
          content_length, content_md5, \
          content_type, headers, sharks, properties) \
          SELECT id, owner, bucket_id, name, created, \
          modified, content_length, \
          content_md5, content_type, headers, sharks, \
          properties FROM manta_bucket_",
        &vnode_str,
        &".manta_bucket_object \
          WHERE owner = $1 \
          AND bucket_id = $2 \
          AND name = $3",
    ]
    .concat()
}

pub fn get_sql(vnode: u64) -> String {
    [
        "SELECT id, owner, bucket_id, name, created, modified, content_length, \
         content_md5, content_type, headers, sharks, properties \
         FROM manta_bucket_",
        &vnode.to_string(),
        &".manta_bucket_object WHERE owner = $1 \
          AND bucket_id = $2 \
          AND name = $3",
    ]
    .concat()
}

#[cfg(test)]
pub mod test {
    use super::*;

    use std::collections::HashMap;

    use chrono::prelude::*;
    use quickcheck::{quickcheck, Arbitrary, Gen};
    use quickcheck_helpers::random;
    use serde_json;
    use serde_json::Map;

    #[derive(Clone, Debug)]
    struct GetObjectJson(Value);
    #[derive(Clone, Debug)]
    struct ObjectJson(Value);

    impl Arbitrary for GetObjectJson {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let owner = serde_json::to_value(Uuid::new_v4())
                .expect("failed to convert owner field to Value");
            let bucket_id = serde_json::to_value(Uuid::new_v4())
                .expect("failed to convert bucket_id field to Value");
            let name = serde_json::to_value(random::string(g, 1024))
                .expect("failed to convert name field to Value");
            let vnode = serde_json::to_value(u64::arbitrary(g))
                .expect("failed to convert vnode field to Value");
            let request_id = serde_json::to_value(Uuid::new_v4())
                .expect("failed to convert request_id field to Value");

            let mut obj = Map::new();
            obj.insert("owner".into(), owner);
            obj.insert("bucket_id".into(), bucket_id);
            obj.insert("name".into(), name);
            obj.insert("vnode".into(), vnode);
            obj.insert("request_id".into(), request_id);
            GetObjectJson(Value::Object(obj))
        }
    }

    impl Arbitrary for GetObjectPayload {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let owner = Uuid::new_v4();
            let bucket_id = Uuid::new_v4();
            let name = random::string(g, 32);
            let vnode = u64::arbitrary(g);
            let request_id = Uuid::new_v4();
            let conditions: conditional::Conditions = Default::default();

            GetObjectPayload {
                owner,
                bucket_id,
                name,
                vnode,
                request_id,
                conditions,
            }
        }
    }

    impl Arbitrary for ObjectResponse {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let id = Uuid::new_v4();
            let owner = Uuid::new_v4();
            let bucket_id = Uuid::new_v4();
            let name = random::string(g, 32);
            let created = Utc::now();
            let modified = Utc::now();
            let content_length = i64::arbitrary(g);
            let content_md5 = random::string(g, 32);
            let content_type = random::string(g, 32);
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

            ObjectResponse {
                id,
                owner,
                bucket_id,
                name,
                created,
                modified,
                content_length,
                content_md5,
                content_type,
                headers,
                sharks,
                properties,
            }
        }
    }

    impl Arbitrary for DeleteObjectResponse {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            Self {
                id: Uuid::new_v4(),
                owner: Uuid::new_v4(),
                bucket_id: Uuid::new_v4(),
                name: random::string(g, 32),
                content_length: i64::arbitrary(g),
                shark_count: i32::arbitrary(g),
            }
        }
    }

    quickcheck! {
        fn prop_get_object_payload_roundtrip(msg: GetObjectPayload) -> bool {
            match serde_json::to_string(&msg) {
                Ok(get_str) => {
                    let decode_result: Result<GetObjectPayload, _> =
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
        fn prop_object_response_roundtrip(msg: ObjectResponse) -> bool {
            match serde_json::to_string(&msg) {
                Ok(response_str) => {
                    let decode_result: Result<ObjectResponse, _> =
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
        fn prop_delete_object_response_roundtrip(dobjr: DeleteObjectResponse) -> bool {
              match serde_json::to_string(&dobjr) {
                 Ok(dobjr_str) => {
                   let decode_result: Result<DeleteObjectResponse, _> =
                         serde_json::from_str(&dobjr_str);
                        match decode_result {
                        Ok(decoded_dobjr) => decoded_dobjr == dobjr,
                            Err(_) => false
                       }
                   },
                  Err(_) => false
             }
        }
    }

    quickcheck! {
        fn prop_getobject_payload_from_json(json: GetObjectJson) -> bool {
            let decode_result1: Result<GetObjectPayload, _> =
                serde_json::from_value(json.0.clone());
            let res1 = decode_result1.is_ok();

            let decode_result2: Result<Vec<GetObjectPayload>, _> =
                serde_json::from_value(Value::Array(vec![json.0]));
            let res2 = decode_result2.is_ok();

            res1 && res2
        }
    }

    quickcheck! {
        fn prop_object_response_to_json(objr: ObjectResponse) -> bool {
            // Test the conversion to JSON. A lack of a panic in the call the
            // `to_json` indicates success.
            let _ = to_json(objr);
            true
        }
    }
}
