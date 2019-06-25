/*
 * Copyright 2019 Joyent, Inc.
 */

use std::io::Error as IOError;
use std::io::ErrorKind as IOErrorKind;

use chrono;
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::error::{BorayError, BorayErrorType};
use crate::util::Rows;

pub mod create;
pub mod delete;
pub mod get;
pub mod list;

type Timestamptz = chrono::DateTime<chrono::Utc>;

#[derive(Serialize, Deserialize)]
pub struct GetBucketPayload {
    pub owner      : Uuid,
    pub name       : String,
    pub vnode      : u64,
    pub request_id : Uuid
}

type DeleteBucketPayload = GetBucketPayload;

#[derive(Serialize, Deserialize, Debug)]
pub struct BucketResponse {
    pub id         : Uuid,
    pub owner      : Uuid,
    pub name       : String ,
    pub created    : Timestamptz
}


pub(self) fn bucket_not_found() -> Value {
    // The data for this JSON conversion is locally controlled
    // so unwrapping the result is ok here.
    serde_json::to_value(BorayError::new(BorayErrorType::BucketNotFound))
        .expect("failed to encode a BucketNotFound error")
}

pub(self) fn bucket_already_exists() -> Value {
    // The data for this JSON conversion is locally controlled
    // so unwrapping the result is ok here.
    serde_json::to_value(BorayError::new(BorayErrorType::BucketAlreadyExists))
        .expect("failed to encode a BucketAlreadyExists error")
}

pub(self) fn response(rows: Rows) -> Result<Option<BucketResponse>, IOError> {
    if rows.is_empty() {
        Ok(None)
    } else if rows.len() == 1 {
        let row = &rows[0];

        //TODO: Valdate # of cols
        let resp = BucketResponse {
            id             : row.get(0),
            owner          : row.get(1),
            name           : row.get(2),
            created        : row.get(3)
        };
        Ok(Some(resp))
    } else {
        let err = format!("Get query found {} results, but expected only 1.",
                          rows.len());
        Err(IOError::new(IOErrorKind::Other, err))
    }
}
