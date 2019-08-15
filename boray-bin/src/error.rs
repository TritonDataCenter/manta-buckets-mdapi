use serde_derive::{Deserialize, Serialize};

pub enum BorayErrorType {
    BucketAlreadyExists,
    BucketNotFound,
    ObjectNotFound,
}

impl ToString for BorayErrorType {
    fn to_string(&self) -> String {
        match *self {
            BorayErrorType::BucketAlreadyExists => "BucketAlreadyExists".into(),
            BorayErrorType::BucketNotFound => "BucketNotFound".into(),
            BorayErrorType::ObjectNotFound => "ObjectNotFound".into(),
        }
    }
}

impl BorayErrorType {
    fn message(&self) -> String {
        match *self {
            BorayErrorType::BucketAlreadyExists => "requested bucket already exists".into(),
            BorayErrorType::BucketNotFound => "requested bucket not found".into(),
            BorayErrorType::ObjectNotFound => "requested object not found".into(),
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct BorayError {
    pub error: BorayInnerError,
}

#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct BorayInnerError {
    pub name: String,
    pub message: String,
}

impl BorayError {
    pub fn new(error: BorayErrorType) -> Self {
        let inner = BorayInnerError {
            name: error.to_string(),
            message: error.message(),
        };
        BorayError { error: inner }
    }
}
