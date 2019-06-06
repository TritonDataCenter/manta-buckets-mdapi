/*
 * Copyright 2019 Joyent, Inc.
 */

use std::io::Error as IOError;
use std::io::ErrorKind as IOErrorKind;

use chrono;
use cueball::connection_pool::ConnectionPool;
use cueball::backend::Backend;
use cueball_static_resolver::StaticIpResolver;
use cueball_postgres_connection::PostgresConnection;
use rust_fast::protocol::{FastMessage, FastMessageData};
use serde_derive::{Deserialize, Serialize};
use serde_json::{json, Value};
use slog::{Logger, debug};
use uuid::Uuid;

use crate::util::Rows;
use crate::sql;

type Timestamptz = chrono::DateTime<chrono::Utc>;

#[derive(Serialize, Deserialize)]
pub struct GetBucketPayload {
    pub owner     : Uuid,
    pub name      : String,
    pub vnode     : u64
}

type DeleteBucketPayload = GetBucketPayload;

#[derive(Serialize, Deserialize, Debug)]
pub struct BucketResponse {
    pub id      : Uuid,
    pub owner   : Uuid,
    pub name    : String ,
    pub created : Timestamptz
}

#[derive(Serialize, Deserialize)]
pub struct CreateBucketPayload {
    pub owner : Uuid,
    pub name  : String,
    pub vnode : u64
}

#[derive(Serialize, Deserialize)]
pub struct ListBucketsPayload {
    pub owner    : Uuid,
    pub vnode    : u64,
    pub prefix   : String,
    pub order_by : String,
    pub limit    : u64,
    pub offset   : u64
}

#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct BucketNotFoundError {
    pub name    : String,
    pub message : String
}

impl BucketNotFoundError {
    pub fn new() -> Self {
        BucketNotFoundError {
            name: "BucketNotFoundError".into(),
            message: "requested bucket not found".into()
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct BucketAlreadyExistsError {
    pub name    : String,
    pub message : String
}

impl BucketAlreadyExistsError {
    pub fn new() -> Self {
        BucketAlreadyExistsError {
            name: "BucketAlreadyExistsError".into(),
            message: "requested bucket already exists".into()
        }
    }
}

fn array_wrap(v: Value) -> Value {
    Value::Array(vec![v])
}

pub fn bucket_not_found() -> Value {
    // The data for this JSON conversion is locally controlled
    // so unwrapping the result is ok here.
    serde_json::to_value(BucketNotFoundError::new())
        .expect("failed to encode a BucketNotFoundError")
}

pub fn bucket_already_exists() -> Value {
    // The data for this JSON conversion is locally controlled
    // so unwrapping the result is ok here.
    serde_json::to_value(BucketAlreadyExistsError::new())
        .expect("failed to encode a BucketAlreadyExistsError")
}

pub fn get_handler(msg_id: u32,
                   args: &[Value],
                   mut response: Vec<FastMessage>,
                   pool: &ConnectionPool<PostgresConnection, StaticIpResolver, impl FnMut(&Backend) -> PostgresConnection + Send + 'static>,
                   log: &Logger) -> Result<Vec<FastMessage>, IOError> {
    debug!(log, "handling getbucket function request");

    let arg0 = match &args[0] {
        Value::Object(_) => &args[0],
        _ => return Err(other_error("Expected JSON object"))
    };

    let data_clone = arg0.clone();
    let payload_result: Result<GetBucketPayload, _> =
        serde_json::from_value(data_clone);

    let payload = match payload_result {
        Ok(o) => o,
        Err(_) => return Err(other_error("Failed to parse JSON data as payload for getbucket function"))
    };

    // Make db request and form response
    get(payload, pool)
        .and_then(|maybe_resp| {
            let method = String::from("getbucket");
            match maybe_resp {
                Some(resp) => {
                    let value = array_wrap(serde_json::to_value(resp).unwrap());
                    let msg = FastMessage::data(msg_id, FastMessageData::new(method, value));
                    response.push(msg);
                    Ok(response)
                },
                None => {

                    let err_msg = FastMessage::error(msg_id, FastMessageData::new(method, bucket_not_found()));
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
    debug!(log, "handling listbuckets function request");

    let arg0 = match &args[0] {
        Value::Object(_) => &args[0],
        _ => return Err(other_error("Expected JSON object"))
    };

    let data_clone = arg0.clone();
    let payload_result: Result<ListBucketsPayload, _> =
        serde_json::from_value(data_clone);

    let payload = match payload_result {
        Ok(o) => o,
        Err(_) => return Err(other_error("Failed to parse JSON data as payload for listbuckets function"))
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
    let list_sql = list_sql(payload.vnode, payload.limit,
        payload.offset, &payload.order_by);

    for row in sql::txn_query(sql::Method::BucketList, &mut txn, list_sql.as_str(),
                              &[&payload.owner,
                              &prefix]).unwrap().iter() {

        let resp = BucketResponse {
            id: row.get(0),
            owner: row.get(1),
            name: row.get(2),
            created: row.get(3)
        };

        let value = array_wrap(serde_json::to_value(resp).unwrap());
        let msg = FastMessage::data(msg_id, FastMessageData::new(String::from("listbuckets"), value));
        response.push(msg);
    }

    Ok(response)
}

pub fn create_handler(msg_id: u32,
                   args: &[Value],
                   mut response: Vec<FastMessage>,
                   pool: &ConnectionPool<PostgresConnection, StaticIpResolver, impl FnMut(&Backend) -> PostgresConnection + Send + 'static>,
                   log: &Logger) -> Result<Vec<FastMessage>, IOError> {
    debug!(log, "handling createbucket function request");

    let arg0 = match &args[0] {
        Value::Object(_) => &args[0],
        _ => return Err(other_error("Expected JSON object"))
    };

    let data_clone = arg0.clone();
    let payload_result: Result<CreateBucketPayload, _> =
        serde_json::from_value(data_clone);

    let payload = match payload_result {
        Ok(o) => o,
        Err(_) => return Err(other_error("Failed to parse JSON data as payload for createbucket function"))
    };

    // Make db request and form response
    create(payload, pool)
        .and_then(|maybe_resp| {
            let method = String::from("createbucket");
            match maybe_resp {
                Some(resp) => {
                    let value = array_wrap(serde_json::to_value(resp).unwrap());
                    let msg = FastMessage::data(msg_id, FastMessageData::new(method, value));
                    response.push(msg);
                    Ok(response)
                },
                None => {
                    let err_msg = FastMessage::error(msg_id, FastMessageData::new(method, bucket_already_exists()));
                    response.push(err_msg);
                    Ok(response)
                }
            }
        })
        //TODO: Proper error handling
        .map_err(|_e| other_error("postgres error"))
}

pub fn delete_handler(msg_id: u32,
                      args: &[Value],
                      mut response: Vec<FastMessage>,
                      pool: &ConnectionPool<PostgresConnection, StaticIpResolver, impl FnMut(&Backend) -> PostgresConnection + Send + 'static>,
                      log: &Logger) -> Result<Vec<FastMessage>, IOError> {
    debug!(log, "handling deletebucket function request");

    let arg0 = match &args[0] {
        Value::Object(_) => &args[0],
        _ => return Err(other_error("Expected JSON object"))
    };

    let data_clone = arg0.clone();
    let payload_result: Result<DeleteBucketPayload, _> =
        serde_json::from_value(data_clone);

    let payload = match payload_result {
        Ok(o) => o,
        Err(_) => return Err(other_error("Failed to parse JSON data as payload \
                                          for deletebucket function"))
    };

    // Make db request and form response
    let response_msg: Result<FastMessage, IOError> =
        delete(payload, pool)
        .and_then(|affected_rows| {
            let method = String::from("deletebucket");
            if affected_rows > 0 {
                let value = array_wrap(serde_json::to_value(affected_rows).unwrap());
                let msg = FastMessage::data(msg_id, FastMessageData::new(method, value));
                Ok(msg)
            } else {
                let err_msg = FastMessage::error(msg_id, FastMessageData::new(method, bucket_not_found()));
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
            let method = String::from("deletebucket");
            let err_msg_data = FastMessageData::new(method, value);
            let err_msg = FastMessage::error(msg_id, err_msg_data);
            Ok(err_msg)
        });

    response.push(response_msg.unwrap());
    Ok(response)
}


fn other_error(msg: &str) -> IOError {
    IOError::new(IOErrorKind::Other, String::from(msg))
}


fn response(rows: Rows) -> Result<Option<BucketResponse>, IOError> {
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

fn get_sql(vnode: u64) -> String {
    ["SELECT id, owner, name, created \
      FROM manta_bucket_",
     &vnode.to_string(),
     &".manta_bucket WHERE owner = $1 \
       AND name = $2"].concat()
}

fn create_sql(vnode: u64) -> String {
    ["INSERT INTO manta_bucket_",
     &vnode.to_string(),
     &".manta_bucket \
       (id, owner, name) \
       VALUES ($1, $2, $3) \
       ON CONFLICT DO NOTHING \
       RETURNING id, owner, name, created"].concat()
}

fn list_sql(vnode: u64, limit: u64, offset: u64, order_by: &str) -> String {
    format!("SELECT id, owner, name, created
        FROM manta_bucket_{}.manta_bucket
        WHERE owner = $1 AND name like $2
        ORDER BY {} ASC
        LIMIT {}
        OFFSET {}",
        vnode, order_by, limit, offset)
}

fn get(payload: GetBucketPayload,
       pool: &ConnectionPool<PostgresConnection, StaticIpResolver, impl FnMut(&Backend) -> PostgresConnection + Send + 'static>)
       -> Result<Option<BucketResponse>, IOError>
{
    let mut conn = pool.claim().unwrap();
    let sql = get_sql(payload.vnode);

    sql::query(sql::Method::BucketGet, &mut conn, sql.as_str(),
               &[&payload.owner,
               &payload.name])
        .map_err(|e| {
           let pg_err = format!("{}", e);
            IOError::new(IOErrorKind::Other, pg_err)
        })
        .and_then(response)
}


fn create(payload: CreateBucketPayload,
       pool: &ConnectionPool<PostgresConnection, StaticIpResolver, impl FnMut(&Backend) -> PostgresConnection + Send + 'static>)
       -> Result<Option<BucketResponse>, IOError>
{
    let mut conn = pool.claim().unwrap();
    let mut txn = (*conn).transaction().unwrap();
    let create_sql = create_sql(payload.vnode);

    let insert_result =
        sql::txn_query(sql::Method::BucketCreate, &mut txn, create_sql.as_str(),
                       &[&Uuid::new_v4(),
                       &payload.owner,
                       &payload.name])
        .map_err(|e| {
           let pg_err = format!("{}", e);
            IOError::new(IOErrorKind::Other, pg_err)
        })
        .and_then(response);

    txn.commit().unwrap();

    insert_result
}

fn insert_delete_table_sql(vnode: u64) -> String {
    let vnode_str = vnode.to_string();
    ["INSERT INTO manta_bucket_",
     &vnode_str,
     &".manta_bucket_deleted_bucket \
      (id, owner, name, created) \
      SELECT id, owner, name, created \
      FROM manta_bucket_",
     &vnode_str,
     &".manta_bucket \
       WHERE owner = $1 \
       AND name = $2"].concat()
}

fn delete_sql(vnode: u64) -> String {
    ["DELETE FROM manta_bucket_",
     &vnode.to_string(),
     &".manta_bucket \
       WHERE owner = $1 \
       AND name = $2"].concat()
}

fn delete(payload: DeleteBucketPayload,
          pool: &ConnectionPool<PostgresConnection, StaticIpResolver, impl FnMut(&Backend) -> PostgresConnection + Send + 'static>)
          -> Result<u64, IOError>
{
    let mut conn = pool.claim().unwrap();
    let mut txn = (*conn).transaction().unwrap();
    let move_sql = insert_delete_table_sql(payload.vnode);
    let delete_sql = delete_sql(payload.vnode);

    sql::txn_execute(sql::Method::BucketDeleteMove, &mut txn, move_sql.as_str(),
                     &[&payload.owner,
                     &payload.name])
        .and_then(|_moved_rows| {
            sql::txn_execute(sql::Method::BucketDelete, &mut txn, delete_sql.as_str(),
                             &[&payload.owner,
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
