/*
 * Copyright 2019 Joyent, Inc.
 */

use std::collections::HashMap;
use std::io::Error as IOError;
use std::io::ErrorKind as IOErrorKind;

use base64;
use chrono;
use postgres::Result as PostgresResult;
use postgres::rows::Rows;
use r2d2::Pool;
use r2d2_postgres::PostgresConnectionManager;
use serde_json::Value;
use slog::Logger;
use uuid::Uuid;

use rust_fast::protocol::{FastMessage, FastMessageData};

type Hstore = HashMap<String, Option<String>>;
type Timestamptz = chrono::DateTime<chrono::Utc>;

#[derive(Debug, Serialize, Deserialize)]
pub struct GetObjectPayload {
    owner     : Uuid,
    bucket_id : Uuid,
    name      : String,
    vnode     : u64
}

type DeleteObjectPayload = GetObjectPayload;

#[derive(Debug, Serialize, Deserialize)]
pub struct ObjectResponse {
    id             : Uuid,
    owner          : Uuid,
    bucket_id      : Uuid,
    name           : String,
    created        : Timestamptz,
    modified       : Timestamptz,
    content_length : i64,
    content_md5    : String,
    content_type   : String,
    headers        : Hstore,
    sharks         : Hstore,
    properties     : Option<Value>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PutObjectPayload {
    owner          : Uuid,
    bucket_id      : Uuid,
    name           : String,
    vnode          : u64,
    content_length : i64,
    content_md5    : String,
    content_type   : String,
    headers        : Hstore,
    sharks         : Hstore,
    properties     : Option<Value>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListObjectsPayload {
    owner     : Uuid,
    bucket_id : Uuid,
    vnode     : u64
}

pub fn get_handler(msg_id: u32,
                      args: &Vec<Value>,
                      mut response: Vec<FastMessage>,
                      pool: &Pool<PostgresConnectionManager>,
                      log: &Logger) -> Result<Vec<FastMessage>, IOError> {
    debug!(log, "handling getobject function request");
    let arg0 = &args[0];
    match arg0 {
        Value::Object(_) => {
            let data_clone = arg0.clone();
            let payload_result: Result<GetObjectPayload, _> =
                serde_json::from_value(data_clone);
            match payload_result {
                Ok(payload) => {
                    // Make db request and form response
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
                                    let value = json!({
                                        "name": "ObjectNotFoundError",
                                        "message": "requested object not found"
                                    });
                                    let err_msg = FastMessage::error(msg_id, FastMessageData::new(method, value));
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
                },
                Err(e) =>
                    Err(other_error(&e.to_string()))
            }
        }
        _ => Err(other_error("Expected JSON object"))
    }
}

pub fn list_handler(msg_id: u32,
                    args: &Vec<Value>,
                    mut response: Vec<FastMessage>,
                    pool: &Pool<PostgresConnectionManager>,
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

    // Make db request and form response
    // TODO: make this call safe
    let conn = pool.get().unwrap();
    let txn = conn.transaction().unwrap();
    let list_sql = list_sql(&payload.vnode);

    for row in txn.query(&list_sql, &[&payload.owner, &payload.bucket_id]).unwrap().iter() {
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
            content_md5    : content_md5,
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
                   args: &Vec<Value>,
                   mut response: Vec<FastMessage>,
                   pool: &Pool<PostgresConnectionManager>,
                   log: &Logger) -> Result<Vec<FastMessage>, IOError> {
    debug!(log, "handling putobject function request");
    let arg0 = &args[0];
    match arg0 {
        Value::Object(_) => {
            let data_clone = arg0.clone();
            let payload_result: Result<PutObjectPayload, _> =
                serde_json::from_value(data_clone);
            match payload_result {
                Ok(payload) => {
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
                },
                Err(_) =>
                    Err(other_error("Failed to parse JSON data as payload for putobject function"))
            }
        }
        _ => Err(other_error("Expected JSON object"))
    }
}

pub fn delete_handler(msg_id: u32,
                      args: &Vec<Value>,
                      mut response: Vec<FastMessage>,
                      pool: &Pool<PostgresConnectionManager>,
                      log: &Logger) -> Result<Vec<FastMessage>, IOError> {
    debug!(log, "handling deleteobject function request");
    let arg0 = &args[0];
    match arg0 {
        Value::Object(_) => {
            let data_clone = arg0.clone();
            let payload_result: Result<DeleteObjectPayload, _> =
                serde_json::from_value(data_clone);
            match payload_result {
                Ok(payload) => {
                    // Make db request and form response
                    delete(payload, pool)
                        .and_then(|resp| {
                            let method = String::from("deleteobject");
                            let value = array_wrap(serde_json::to_value(resp).unwrap());
                            let msg = FastMessage::data(msg_id, FastMessageData::new(method, value));
                            response.push(msg);
                            Ok(response)
                        })
                        //TODO: Proper error handling
                        .map_err(|_e| other_error("postgres error"))
                },
                Err(_) =>
                    Err(other_error("Failed to parse JSON data as payload for putobject function"))
            }
        }
        _ => Err(other_error("Expected JSON object"))
    }
}

fn array_wrap(v: Value) -> Value {
    Value::Array(vec![v])
}

fn other_error(msg: &str) -> IOError {
    IOError::new(IOErrorKind::Other, String::from(msg))
}

fn get(payload: GetObjectPayload, pool: &Pool<PostgresConnectionManager>)
                     -> PostgresResult<Option<ObjectResponse>>
{
    let conn = pool.get().unwrap();
    let sql = get_sql(&payload.vnode);
    conn.query(&sql, &[&payload.owner,
                       &payload.bucket_id,
                       &payload.name])
        .and_then(|rows| response(rows))
        .map_err(|e| e)
}

fn response(rows: Rows) -> PostgresResult<Option<ObjectResponse>> {
    if rows.len() == 0 {
        Ok(None)
    } else if rows.len() == 1 {
        let row = rows.get(0);
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
            content_md5    : content_md5,
            content_type   : row.get(8),
            headers        : row.get(9),
            sharks         : row.get(10),
            properties     : row.get(11),
        };
        Ok(Some(resp))
    } else {
        let err = format!("Get query found {} results, but expected only 1.",
                          rows.len());
        Err(IOError::new(IOErrorKind::Other, err).into())
    }
}

fn get_sql(vnode: &u64) -> String {
    ["SELECT id, owner, bucket_id, name, created, modified, content_length, \
      content_md5, content_type, headers, sharks, properties \
      FROM manta_bucket_",
     &vnode.to_string(),
     &".manta_bucket_object WHERE owner = $1 \
       AND bucket_id = $2 \
       AND name = $3"].concat()
}

// TODO add limits to this
fn list_sql(vnode: &u64) -> String {
    ["SELECT id, owner, bucket_id, name, created, modified, content_length, \
      content_md5, content_type, headers, sharks, properties \
      FROM manta_bucket_",
     &vnode.to_string(),
     &".manta_bucket_object WHERE owner = $1 \
       AND bucket_id = $2"].concat()
}

fn put(payload: PutObjectPayload, pool: &Pool<PostgresConnectionManager>)
       -> PostgresResult<Option<ObjectResponse>>
{
    let conn = pool.get().unwrap();
    let txn = conn.transaction().unwrap();
    let put_sql = put_sql(&payload.vnode);
    let move_sql = insert_delete_table_sql(&payload.vnode);
    let content_md5_bytes = base64::decode(&payload.content_md5).unwrap();
    let result = txn.execute(&move_sql, &[&payload.owner,
                                          &payload.bucket_id,
                                          &payload.name])
        .and_then(|_moved_rows| {
            txn.query(&put_sql, &[&Uuid::new_v4(),
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
        .and_then(|rows| response(rows))
        .and_then(|response| {
            let _commit_result = txn.commit().unwrap();
            Ok(response)
        })
        .map_err(|e| {
            let pg_err = format!("{}", e);
            IOError::new(IOErrorKind::Other, pg_err).into()
        });
    result
}

fn insert_delete_table_sql(vnode: &u64) -> String {
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

fn put_sql(vnode: &u64) -> String {
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

fn delete_sql(vnode: &u64) -> String {
    ["DELETE FROM manta_bucket_",
     &vnode.to_string(),
     &".manta_bucket_object \
       WHERE owner = $1 \
       AND bucket_id = $2 \
       AND name = $3"].concat()
}

fn delete(payload: DeleteObjectPayload, pool: &Pool<PostgresConnectionManager>)
          -> PostgresResult<u64>
{
    let conn = pool.get().unwrap();
    let txn = conn.transaction().unwrap();
    let move_sql = insert_delete_table_sql(&payload.vnode);
    let delete_sql = delete_sql(&payload.vnode);
    let result = txn.execute(&move_sql, &[&payload.owner,
                                          &payload.bucket_id,
                                          &payload.name])
        .and_then(|_moved_rows| {
            txn.execute(&delete_sql, &[&payload.owner,
                                       &payload.bucket_id,
                                       &payload.name])
        })
        .and_then(|row_count| {
            let _commit_result = txn.commit().unwrap();
            Ok(row_count)
        })
        .map_err(|e| {
            let pg_err = format!("{}", e);
            IOError::new(IOErrorKind::Other, pg_err).into()
        });
    result
}
