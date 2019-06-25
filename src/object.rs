/*
 * Copyright 2019 Joyent, Inc.
 */

use std::error::Error;
use std::io::Error as IOError;
use std::io::ErrorKind as IOErrorKind;
use std::vec::Vec;

use base64;
use postgres::types::{FromSql, IsNull, ToSql, Type};
use tokio_postgres::{accepts, to_sql_checked};
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::error::{BorayError, BorayErrorType};
use crate::util::{
    Hstore,
    Rows,
    Timestamptz
};

pub mod create;
pub mod delete;
pub mod get;
pub mod list;
pub mod update;

#[derive(Debug, Serialize, Deserialize)]
pub struct GetObjectPayload {
    pub owner      : Uuid,
    pub bucket_id  : Uuid,
    pub name       : String,
    pub vnode      : u64,
    pub request_id : Uuid
}

type DeleteObjectPayload = GetObjectPayload;

/// A type that represents the information about the datacenter and storage node
/// id of a copy of an object's data.
///
/// The incoming representation of the sharks data is a JSON array of objects
/// where each object has two keys: datacenter and manta_storage_id. The custom
/// ToSQL instance for the this type converts the each object in this
/// representation into a String that represents the same data in fewer
/// bytes. The postgres column type for the sharks column is a text array.
///
/// Likewise, the custom FromSql instance for the type converts the members of
/// the text array format stored in the database back into an instance of
/// StorageNodeIdentifier.
#[derive(Debug, Serialize, Deserialize)]
pub struct StorageNodeIdentifier {
    pub datacenter: String,
    pub manta_storage_id: String
}

impl ToString for StorageNodeIdentifier {
    fn to_string(&self) -> String {
        [&self.datacenter, ":", &self.manta_storage_id].concat()
    }
}

impl From<String> for StorageNodeIdentifier {
    fn from(s: String) -> Self {
        let v: Vec<&str> = s.split(':').collect();
        StorageNodeIdentifier {
            datacenter: String::from(v[0]),
            manta_storage_id: String::from(v[1])
        }
    }
}

impl ToSql for StorageNodeIdentifier {
    fn to_sql(&self, ty: &Type, w: &mut Vec<u8>) ->
        Result<IsNull, Box<dyn Error + Sync + Send>> {
            <String as ToSql>::to_sql(&self.to_string(), ty, w)
    }

    accepts!(TEXT);

    to_sql_checked!();
}

impl<'a> FromSql<'a> for StorageNodeIdentifier {
    fn from_sql(ty: &Type, raw: &'a [u8]) ->
        Result<StorageNodeIdentifier, Box<dyn Error + Sync + Send>> {
            String::from_sql(ty, raw)
                .and_then(|s| {
                    Ok(StorageNodeIdentifier::from(s))
                })
    }

    accepts!(TEXT);
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ObjectResponse {
    pub id             : Uuid,
    pub owner          : Uuid,
    pub bucket_id      : Uuid,
    pub name           : String,
    pub created        : Timestamptz,
    pub modified       : Timestamptz,
    pub content_length : i64,
    pub content_md5    : String,
    pub content_type   : String,
    pub headers        : Hstore,
    pub sharks         : Vec<StorageNodeIdentifier>,
    pub properties     : Option<Value>
}

pub(self) fn object_not_found() -> Value {
    // The data for this JSON conversion is locally controlled
    // so unwrapping the result is ok here.
    serde_json::to_value(BorayError::new(BorayErrorType::ObjectNotFound))
        .expect("failed to encode a ObjectNotFound error")
}

pub(self) fn response(rows: Rows) -> Result<Option<ObjectResponse>, IOError> {
    if rows.is_empty() {
        Ok(None)
    } else if rows.len() == 1 {
        let row = &rows[0];
        let content_md5_bytes: Vec<u8> = row.get(7);
        let content_md5 = base64::encode(&content_md5_bytes);
        //TODO: Valdate # of cols
        let resp = ObjectResponse {
            id             : row.get(0),
            owner          : row.get(1),
            bucket_id      : row.get(2),
            name           : row.get(3),
            created        : row.get(4),
            modified       : row.get(5),
            content_length : row.get(6),
            content_md5,
            content_type   : row.get(8),
            headers        : row.get(9),
            sharks         : row.get(10),
            properties     : row.get(11),
        };
        Ok(Some(resp))
    } else {
        let err = format!("Get query found {} results, but expected only 1.",
                          rows.len());
        Err(IOError::new(IOErrorKind::Other, err))
    }
}

pub(self) fn insert_delete_table_sql(vnode: u64) -> String {
    let vnode_str = vnode.to_string();
    ["INSERT INTO manta_bucket_",
     &vnode_str,
     &".manta_bucket_deleted_object ( \
      id, owner, bucket_id, name, created, modified, \
      creator, content_length, content_md5, \
      content_type, headers, sharks, properties) \
      SELECT id, owner, bucket_id, name, created, \
      modified, creator, content_length, \
      content_md5, content_type, headers, sharks, \
      properties FROM manta_bucket_",
     &vnode_str,
     &".manta_bucket_object \
       WHERE owner = $1 \
       AND bucket_id = $2 \
       AND name = $3"].concat()
}
