/*
 * Copyright 2019 Joyent, Inc.
 */

use std::collections::HashMap;
use std::io::Error as IOError;
use std::io::ErrorKind as IOErrorKind;
use std::vec::Vec;

use base64;
use chrono;
use cueball::connection_pool::ConnectionPool;
use cueball::backend::Backend;
use cueball_static_resolver::StaticIpResolver;
use cueball_postgres_connection::PostgresConnection;
use rust_fast::protocol::{FastMessage, FastMessageData};
use serde_derive::{Deserialize, Serialize};
use serde_json::{Value, json};
use slog::{Logger, debug};
use uuid::Uuid;

use crate::util::Rows;

type Hstore = HashMap<String, Option<String>>;
type TextArray = Vec<String>;
type Timestamptz = chrono::DateTime<chrono::Utc>;

#[derive(Debug, Serialize, Deserialize)]
pub struct GetObjectPayload {
    pub owner     : Uuid,
    pub bucket_id : Uuid,
    pub name      : String,
    pub vnode     : u64
}

type DeleteObjectPayload = GetObjectPayload;

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
    pub sharks         : TextArray,
    pub properties     : Option<Value>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PutObjectPayload {
    pub owner          : Uuid,
    pub bucket_id      : Uuid,
    pub name           : String,
    pub vnode          : u64,
    pub content_length : i64,
    pub content_md5    : String,
    pub content_type   : String,
    pub headers        : Hstore,
    pub sharks         : TextArray,
    pub properties     : Option<Value>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListObjectsPayload {
    pub owner     : Uuid,
    pub bucket_id : Uuid,
    pub vnode     : u64,
    pub prefix    : String,
    pub order_by  : String,
    pub limit     : u64,
    pub offset    : u64
}

#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct ObjectNotFoundError {
    pub name    : String,
    pub message : String
}

impl ObjectNotFoundError {
    pub fn new() -> Self {
        ObjectNotFoundError {
            name: "ObjectNotFoundError".into(),
            message: "requested object not found".into()
        }
    }
}

fn object_not_found() -> Value {
    // The data for this JSON conversion is locally controlled
    // so unwrapping the result is ok here.
    serde_json::to_value(ObjectNotFoundError::new())
        .expect("failed to encode a BucketNotFound Error")
}

pub fn get_handler(msg_id: u32,
                   args: &[Value],
                   mut response: Vec<FastMessage>,
                   pool: &ConnectionPool<PostgresConnection, StaticIpResolver, impl FnMut(&Backend) -> PostgresConnection + Send + 'static>,
                      log: &Logger) -> Result<Vec<FastMessage>, IOError> {
    debug!(log, "handling getobject function request");

    let arg0 = match &args[0] {
        Value::Object(_) => &args[0],
        _ => return Err(other_error("Expected JSON object"))
    };

    let data_clone = arg0.clone();
    let payload_result: Result<GetObjectPayload, _> =
        serde_json::from_value(data_clone);

    let payload = match payload_result {
        Ok(o) => o,
        Err(_) => return Err(other_error("Failed to parse JSON data as payload for getobject function"))
    };

    get(payload, pool)
        .and_then(|maybe_resp| {
            let method = String::from("getobject");
            match maybe_resp {
                Some(resp) => {
                    let value = array_wrap(serde_json::to_value(resp).unwrap());
                    let msg = FastMessage::data(msg_id, FastMessageData::new(method, value));
                    response.push(msg);
                    Ok(response)
                },
                None => {
                    let err_msg = FastMessage::error(msg_id, FastMessageData::new(method, object_not_found()));
                    response.push(err_msg);
                    Ok(response)
                }
            }
        })
        //TODO: Proper error handling
        .map_err(|e| {
            println!("Error: {}", e);
            other_error("postgres error")
        })
}

pub fn list_handler(msg_id: u32,
                    args: &[Value],
                    mut response: Vec<FastMessage>,
                    pool: &ConnectionPool<PostgresConnection, StaticIpResolver, impl FnMut(&Backend) -> PostgresConnection + Send + 'static>,
                    log: &Logger) -> Result<Vec<FastMessage>, IOError> {
    debug!(log, "handling listobjects function request");

    let arg0 = match &args[0] {
        Value::Object(_) => &args[0],
        _ => return Err(other_error("Expected JSON object"))
    };

    let data_clone = arg0.clone();
    let payload_result: Result<ListObjectsPayload, _> =
        serde_json::from_value(data_clone);

    let payload = match payload_result {
        Ok(o) => o,
        Err(_) => return Err(other_error("Failed to parse JSON data as payload for listobjects function"))
    };

    // TODO catch these as errors and return to the caller
    assert!(payload.limit > 0);
    assert!(payload.limit <= 1000);

    match payload.order_by.as_ref() {
        "created" | "name" => {},
        _ => return Err(other_error("Unexpected value for payload.order_by"))
    }

    let prefix = format!("{}%", &payload.prefix);

    // Make db request and form response
    // TODO: make this call safe
    let mut conn = pool.claim().unwrap();
    let mut txn = (*conn).transaction().unwrap();
    let list_sql = list_sql(payload.vnode, payload.limit, payload.offset,
        &payload.order_by);

    for row in txn.query(list_sql.as_str(), &[&payload.owner, &payload.bucket_id, &prefix]).unwrap().iter() {
        let content_md5_bytes: Vec<u8> = row.get(7);
        let content_md5 = base64::encode(&content_md5_bytes);
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

        let value = array_wrap(serde_json::to_value(resp).unwrap());
        let msg = FastMessage::data(msg_id, FastMessageData::new(String::from("listobjects"), value));
        response.push(msg);
    }

    Ok(response)
}

pub fn put_handler(msg_id: u32,
                   args: &[Value],
                   mut response: Vec<FastMessage>,
                   pool: &ConnectionPool<PostgresConnection, StaticIpResolver, impl FnMut(&Backend) -> PostgresConnection + Send + 'static>,
                   log: &Logger) -> Result<Vec<FastMessage>, IOError> {
    debug!(log, "handling putobject function request");

    let arg0 = match &args[0] {
        Value::Object(_) => &args[0],
        _ => return Err(other_error("Expected JSON object"))
    };

    let data_clone = arg0.clone();
    let payload_result: Result<PutObjectPayload, _> =
        serde_json::from_value(data_clone);

    let payload = match payload_result {
        Ok(o) => o,
        Err(_) => return Err(other_error("Failed to parse JSON data as payload for putobject function"))
    };

    // Make db request and form response
    put(payload, pool)
        .and_then(|resp| {
            let method = String::from("putobject");
            let value = array_wrap(serde_json::to_value(resp).unwrap());
            let msg = FastMessage::data(msg_id, FastMessageData::new(method, value));
            response.push(msg);
            Ok(response)
        })
        //TODO: Proper error handling
        .map_err(|_e| other_error("postgres error"))
}

pub fn delete_handler(msg_id: u32,
                      args: &[Value],
                      mut response: Vec<FastMessage>,
                      pool: &ConnectionPool<PostgresConnection, StaticIpResolver, impl FnMut(&Backend) -> PostgresConnection + Send + 'static>,
                      log: &Logger) -> Result<Vec<FastMessage>, IOError> {
    debug!(log, "handling deleteobject function request");

    let arg0 = match &args[0] {
        Value::Object(_) => &args[0],
        _ => return Err(other_error("Expected JSON object"))
    };

    let data_clone = arg0.clone();
    let payload_result: Result<DeleteObjectPayload, _> =
        serde_json::from_value(data_clone);

    let payload = match payload_result {
        Ok(o) => o,
        Err(_) => return Err(other_error("Failed to parse JSON data as payload \
                                          for deleteobject function"))
    };

    // Make db request and form response
    let response_msg: Result<FastMessage, IOError> =
        delete(payload, pool)
        .and_then(|affected_rows| {
            let method = String::from("deleteobject");
            if affected_rows > 0 {
                let value = array_wrap(serde_json::to_value(affected_rows).unwrap());
                let msg = FastMessage::data(msg_id, FastMessageData::new(method, value));
                Ok(msg)
            } else {
                let err_msg = FastMessage::error(msg_id, FastMessageData::new(method, object_not_found()));
                Ok(err_msg)
            }
        })
        .or_else(|e| {
            // TODO: Write a helper function to deconstruct the postgres::Error
            // and populate meaningful name and message fields for the error
            // dependent on the details of the postgres error.
            let err_str = format!("{}", e);
            let value = array_wrap(json!({
                "name": "PostgresError",
                "message": err_str
            }));
            let method = String::from("deleteobject");
            let err_msg_data = FastMessageData::new(method, value);
            let err_msg = FastMessage::error(msg_id, err_msg_data);
            Ok(err_msg)
        });

    response.push(response_msg.unwrap());
    Ok(response)
}

fn array_wrap(v: Value) -> Value {
    Value::Array(vec![v])
}

fn other_error(msg: &str) -> IOError {
    IOError::new(IOErrorKind::Other, String::from(msg))
}

fn get(payload: GetObjectPayload,
       pool: &ConnectionPool<PostgresConnection, StaticIpResolver, impl FnMut(&Backend) -> PostgresConnection + Send + 'static>)
       -> Result<Option<ObjectResponse>, IOError>
{
    let mut conn = pool.claim().unwrap();
    let sql = get_sql(payload.vnode);
    (*conn).query(sql.as_str(), &[&payload.owner,
                                  &payload.bucket_id,
                                  &payload.name])
        .map_err(|e| {
            let pg_err = format!("{}", e);
            IOError::new(IOErrorKind::Other, pg_err)
        })
        .and_then(response)
}

fn response(rows: Rows) -> Result<Option<ObjectResponse>, IOError> {
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

fn get_sql(vnode: u64) -> String {
    ["SELECT id, owner, bucket_id, name, created, modified, content_length, \
      content_md5, content_type, headers, sharks, properties \
      FROM manta_bucket_",
     &vnode.to_string(),
     &".manta_bucket_object WHERE owner = $1 \
       AND bucket_id = $2 \
       AND name = $3"].concat()
}

fn list_sql(vnode: u64, limit: u64, offset: u64, order_by: &str) -> String {
    format!("SELECT id, owner, bucket_id, name, created, modified, \
        content_length, content_md5, content_type, headers, sharks, \
        properties \
        FROM manta_bucket_{}.manta_bucket_object
        WHERE owner = $1 AND bucket_id = $2 AND name like $3
        ORDER BY {} ASC
        LIMIT {}
        OFFSET {}",
        vnode, order_by, limit, offset)
}

fn put(payload: PutObjectPayload,
       pool: &ConnectionPool<PostgresConnection, StaticIpResolver, impl FnMut(&Backend) -> PostgresConnection + Send + 'static>)
       -> Result<Option<ObjectResponse>, IOError>
{
    let mut conn = pool.claim().unwrap();
    let mut txn = (*conn).transaction().unwrap();
    let put_sql = put_sql(payload.vnode);
    let move_sql = insert_delete_table_sql(payload.vnode);
    let content_md5_bytes = base64::decode(&payload.content_md5).unwrap();
    txn.execute(move_sql.as_str(), &[&payload.owner,
                                     &payload.bucket_id,
                                     &payload.name])
        .and_then(|_moved_rows| {
            txn.query(put_sql.as_str(), &[&Uuid::new_v4(),
                                          &payload.owner,
                                          &payload.bucket_id,
                                          &payload.name,
                                          &payload.content_length,
                                          &content_md5_bytes,
                                          &payload.content_type,
                                          &payload.headers,
                                          &payload.sharks,
                                          &payload.properties])
        })
        .map_err(|e| {
            let pg_err = format!("{}", e);
            IOError::new(IOErrorKind::Other, pg_err)
        })
        .and_then(response)
        .and_then(|response| {
            txn.commit().unwrap();
            Ok(response)
        })
}

fn insert_delete_table_sql(vnode: u64) -> String {
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

fn put_sql(vnode: u64) -> String {
    ["INSERT INTO manta_bucket_",
     &vnode.to_string(),
     &".manta_bucket_object ( \
       id, owner, bucket_id, name, content_length, content_md5, \
       content_type, headers, sharks, properties) \
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
       ON CONFLICT (owner, bucket_id, name) DO UPDATE \
       SET id = EXCLUDED.id, \
       created = current_timestamp, \
       modified = current_timestamp, \
       content_length = EXCLUDED.content_length, \
       content_md5 = EXCLUDED.content_md5, \
       content_type = EXCLUDED.content_type, \
       headers = EXCLUDED.headers, \
       sharks = EXCLUDED.sharks, \
       properties = EXCLUDED.properties \
       RETURNING id, owner, bucket_id, name, created, modified, \
       content_length, content_md5, content_type, headers, \
       sharks, properties"].concat()
}

fn delete_sql(vnode: u64) -> String {
    ["DELETE FROM manta_bucket_",
     &vnode.to_string(),
     &".manta_bucket_object \
       WHERE owner = $1 \
       AND bucket_id = $2 \
       AND name = $3"].concat()
}

fn delete(payload: DeleteObjectPayload,
          pool: &ConnectionPool<PostgresConnection, StaticIpResolver, impl FnMut(&Backend) -> PostgresConnection + Send + 'static>)
          -> Result<u64, IOError>
{
    let mut conn = pool.claim().unwrap();
    let mut txn = (*conn).transaction().unwrap();
    let move_sql = insert_delete_table_sql(payload.vnode);
    let delete_sql = delete_sql(payload.vnode);
    txn.execute(move_sql.as_str(), &[&payload.owner,
                                     &payload.bucket_id,
                                     &payload.name])
        .and_then(|_moved_rows| {
            txn.execute(delete_sql.as_str(), &[&payload.owner,
                                               &payload.bucket_id,
                                               &payload.name])
        })
        .and_then(|row_count| {
            txn.commit().unwrap();
            Ok(row_count)
        })
        .map_err(|e| {
            let pg_err = format!("{}", e);
            IOError::new(IOErrorKind::Other, pg_err)
        })
}
