use serde_derive::{Deserialize, Serialize};

#[derive(Clone, Copy)]
pub enum BorayErrorType {
    BucketAlreadyExists,
    BucketNotFound,
    ObjectNotFound,
    PostgresError,
    LimitConstraintError,
}

impl ToString for BorayErrorType {
    fn to_string(&self) -> String {
        match *self {
            BorayErrorType::BucketAlreadyExists => "BucketAlreadyExists".into(),
            BorayErrorType::BucketNotFound => "BucketNotFound".into(),
            BorayErrorType::ObjectNotFound => "ObjectNotFound".into(),
            BorayErrorType::PostgresError => "PostgresError".into(),
            BorayErrorType::LimitConstraintError => "LimitConstraintError".into(),
        }
    }
}

impl BorayErrorType {
    fn message(self) -> String {
        match self {
            BorayErrorType::BucketAlreadyExists => "requested bucket already exists".into(),
            BorayErrorType::BucketNotFound => "requested bucket not found".into(),
            BorayErrorType::ObjectNotFound => "requested object not found".into(),
            BorayErrorType::PostgresError => "postgres encountered an error".into(),
            BorayErrorType::LimitConstraintError => "a limit constraint was violated".into(),
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
        Self::with_message(error, error.message())
    }

    pub fn with_message(error: BorayErrorType, msg: String) -> Self {
        let inner = BorayInnerError {
            name: error.to_string(),
            message: msg,
        };
        Self { error: inner }
    }
}
