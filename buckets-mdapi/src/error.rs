// Copyright 2020 Joyent, Inc.

use serde_derive::{Serialize, Deserialize};
use serde_json::{Value};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum BucketsMdapiError {
    BucketAlreadyExists,
    BucketNotFound,
    ObjectNotFound,
    LimitConstraintError(String),
    PreconditionFailedError(String),
    PostgresError(String),
    ContentMd5Error(String),
}

impl ToString for BucketsMdapiError {
    fn to_string(&self) -> String {
        match *self {
            BucketsMdapiError::BucketAlreadyExists => "BucketAlreadyExists".into(),
            BucketsMdapiError::BucketNotFound => "BucketNotFound".into(),
            BucketsMdapiError::ObjectNotFound => "ObjectNotFound".into(),
            BucketsMdapiError::LimitConstraintError(_) => "LimitConstraintError".into(),
            BucketsMdapiError::PreconditionFailedError(_) => "PreconditionFailedError".into(),
            BucketsMdapiError::PostgresError(_) => "PostgresError".into(),
            BucketsMdapiError::ContentMd5Error(_) => "ContentMd5Error".into(),
        }
    }
}

impl BucketsMdapiError {
    pub fn message(&self) -> String {
        match self {
            BucketsMdapiError::BucketAlreadyExists => "requested bucket already exists".into(),
            BucketsMdapiError::BucketNotFound => "requested bucket not found".into(),
            BucketsMdapiError::ObjectNotFound => "requested object not found".into(),
            BucketsMdapiError::LimitConstraintError(msg) => msg.to_string(),
            BucketsMdapiError::PreconditionFailedError(msg) => msg.to_string(),
            BucketsMdapiError::PostgresError(msg) => msg.to_string(),
            BucketsMdapiError::ContentMd5Error(msg) => {
                format!("content_md5 is not valid base64 encoded data: {}", msg)
            },
        }
    }

    /*
     * The Fast protocol expects our errors to look like so:
     *
     *     { "error": { "name": "...", "message": "..." } }
     *
     * We can get most of the way there by annotating BucketsMdapiError with Serde's tag/content
     * for enum {de,}serialization, but it falls a bit short in its current form because not all
     * elements include a String variant.  This means that "message" is either not present while
     * serializing, or causes an error while deserializing.
     *
     * Paired with the fact that we also need the outer "error" property, it seems the easiest way
     * to get that without writing custom {de,}serializers is to just wrap the error in a few more
     * structs and provide this convenience method to do that wrapping for us.
     */
    pub fn into_fast(self) -> Value {
        serde_json::to_value(BucketsMdapiWrappedError::new(self))
            .expect("failed to encode BucketsMdapiError")
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct BucketsMdapiWrappedError {
    pub error: BucketsMdapiInnerError,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct BucketsMdapiInnerError {
    pub name: String,
    pub message: String,
}

impl BucketsMdapiWrappedError {
    pub fn new(error: BucketsMdapiError) -> Self {
        let inner = BucketsMdapiInnerError {
            name: error.to_string(),
            message: error.message(),
        };
        Self { error: inner }
    }
}
