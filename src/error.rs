use serde_derive::{Deserialize, Serialize};

#[derive(Clone, Copy)]
pub enum BorayErrorType {
    BucketAlreadyExists,
    BucketNotFound,
    ObjectNotFound,
}

impl ToString for BorayErrorType {
    fn to_string(&self) -> String {
        match *self {
            Self::BucketAlreadyExists => "BucketAlreadyExists".into(),
            Self::BucketNotFound => "BucketNotFound".into(),
            Self::ObjectNotFound => "ObjectNotFound".into(),
        }
    }
}

impl BorayErrorType {
    fn message(self) -> String {
        match self {
            Self::BucketAlreadyExists => "requested bucket already exists".into(),
            Self::BucketNotFound => "requested bucket not found".into(),
            Self::ObjectNotFound => "requested object not found".into(),
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
        Self { error: inner }
    }
}
