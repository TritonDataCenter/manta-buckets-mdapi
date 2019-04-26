/*
 * Copyright 2019 Joyent, Inc.
 */

use std::io::Error as IOError;
use std::io::ErrorKind as IOErrorKind;

use chrono;
use r2d2::Pool;
use postgres::Result as PostgresResult;
use postgres::rows::Rows;
use r2d2_postgres::PostgresConnectionManager;
use serde_derive::{Deserialize, Serialize};
use serde_json::{json, Value};
use slog::{Logger, debug};
use uuid::Uuid;

use rust_fast::protocol::{FastMessage, FastMessageData};

type Timestamptz = chrono::DateTime<chrono::Utc>;

#[derive(Serialize, Deserialize)]
pub struct GetBucketPayload {
    owner     : Uuid,
    name      : String,
    vnode     : u64
}

type DeleteBucketPayload = GetBucketPayload;

#[derive(Serialize, Deserialize, Debug)]
pub struct BucketResponse {
    id      : Uuid,
    owner   : Uuid,
    name    : String ,
    created : Timestamptz
}

#[derive(Serialize, Deserialize)]
pub struct PutBucketPayload {
    owner : Uuid,
    name  : String,
    vnode : u64
}

#[derive(Serialize, Deserialize)]
pub struct ListBucketsPayload {
    owner    : Uuid,
    vnode    : u64,
    prefix   : String,
    order_by : String,
    limit    : u64,
    offset   : u64
}

fn array_wrap(v: Value) -> Value {
    Value::Array(vec![v])
}

pub fn get_handler(msg_id: u32,
                   args: &Vec<Value>,
                   mut response: Vec<FastMessage>,
                   pool: &Pool<PostgresConnectionManager>,
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
                    let value = json!({
                        "name": "BucketNotFoundError",
                        "message": "requested bucket not found"
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
}

pub fn list_handler(msg_id: u32,
                    args: &Vec<Value>,
                    mut response: Vec<FastMessage>,
                    pool: &Pool<PostgresConnectionManager>,
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
    let conn = pool.get().unwrap();
    let txn = conn.transaction().unwrap();
    let list_sql = list_sql(&payload.vnode, &payload.limit,
        &payload.offset, &payload.order_by);

    for row in txn.query(&list_sql, &[&payload.owner, &prefix]).unwrap().iter() {
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

pub fn put_handler(msg_id: u32,
                   args: &Vec<Value>,
                   mut response: Vec<FastMessage>,
                   pool: &Pool<PostgresConnectionManager>,
                   log: &Logger) -> Result<Vec<FastMessage>, IOError> {
    debug!(log, "handling putbucket function request");

    let arg0 = match &args[0] {
        Value::Object(_) => &args[0],
        _ => return Err(other_error("Expected JSON object"))
    };

    let data_clone = arg0.clone();
    let payload_result: Result<PutBucketPayload, _> =
        serde_json::from_value(data_clone);

    let payload = match payload_result {
        Ok(o) => o,
        Err(_) => return Err(other_error("Failed to parse JSON data as payload for putbucket function"))
    };

    // Make db request and form response
    put(payload, pool)
        .and_then(|maybe_resp| {
            let method = String::from("putbucket");
            match maybe_resp {
                Some(resp) => {
                    let value = array_wrap(serde_json::to_value(resp).unwrap());
                    let msg = FastMessage::data(msg_id, FastMessageData::new(method, value));
                    response.push(msg);
                    Ok(response)
                },
                None => {
                    let value = json!({
                        "name": "BucketAlreadyExistsError",
                        "message": "requested bucket already exists"
                    });
                    let err_msg = FastMessage::error(msg_id, FastMessageData::new(method, value));
                    response.push(err_msg);
                    Ok(response)
                }
            }
        })
        //TODO: Proper error handling
        .map_err(|_e| other_error("postgres error"))
}

pub fn delete_handler(msg_id: u32,
                      args: &Vec<Value>,
                      mut response: Vec<FastMessage>,
                      pool: &Pool<PostgresConnectionManager>,
                      log: &Logger) -> Result<Vec<FastMessage>, IOError> {
    debug!(log, "handling putbucket function request");

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
                let value = json!({
                    "name": "BucketNotFoundError",
                    "message": "requested bucket not found"
                });
                let err_msg = FastMessage::error(msg_id, FastMessageData::new(method, value));
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


fn response(rows: Rows) -> PostgresResult<Option<BucketResponse>> {
    if rows.len() == 0 {
        Ok(None)
    } else if rows.len() == 1 {
        let row = rows.get(0);
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
        Err(IOError::new(IOErrorKind::Other, err).into())
    }
}

fn get_sql(vnode: &u64) -> String {
    ["SELECT id, owner, name, created \
      FROM manta_bucket_",
     &vnode.to_string(),
     &".manta_bucket WHERE owner = $1 \
       AND name = $2"].concat()
}

fn put_sql(vnode: &u64) -> String {
    ["INSERT INTO manta_bucket_",
     &vnode.to_string(),
     &".manta_bucket \
       (id, owner, name) \
       VALUES ($1, $2, $3) \
       ON CONFLICT DO NOTHING \
       RETURNING id, owner, name, created"].concat()
}

fn list_sql(vnode: &u64, limit: &u64, offset: &u64, order_by: &str) -> String {
    format!("SELECT id, owner, name, created
        FROM manta_bucket_{}.manta_bucket
        WHERE owner = $1 AND name like $2
        ORDER BY {} ASC
        LIMIT {}
        OFFSET {}",
        vnode, order_by, limit, offset)
}

fn get(payload: GetBucketPayload, pool: &Pool<PostgresConnectionManager>)
           -> PostgresResult<Option<BucketResponse>>
{
    let conn = pool.get().unwrap();
    let sql = get_sql(&payload.vnode);
    conn.query(&sql, &[&payload.owner,
                       &payload.name])
        .and_then(|rows| response(rows))
        .map_err(|e| e)
}


fn put(payload: PutBucketPayload, pool: &Pool<PostgresConnectionManager>)
           -> PostgresResult<Option<BucketResponse>>
{
    let conn = pool.get().unwrap();
    let txn = conn.transaction().unwrap();
    let put_sql = put_sql(&payload.vnode);

    let insert_result =
        txn.query(&put_sql, &[&Uuid::new_v4(),
                              &payload.owner,
                              &payload.name])
        .and_then(|rows| response(rows))
        .map_err(|e| {
           let pg_err = format!("{}", e);
            IOError::new(IOErrorKind::Other, pg_err).into()
        });

    let _commit_result = txn.commit().unwrap();

    insert_result
}

fn insert_delete_table_sql(vnode: &u64) -> String {
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

fn delete_sql(vnode: &u64) -> String {
    ["DELETE FROM manta_bucket_",
     &vnode.to_string(),
     &".manta_bucket \
       WHERE owner = $1 \
       AND name = $2"].concat()
}

fn delete(payload: DeleteBucketPayload, pool: &Pool<PostgresConnectionManager>)
          -> PostgresResult<u64>
{
    let conn = pool.get().unwrap();
    let txn = conn.transaction().unwrap();
    let move_sql = insert_delete_table_sql(&payload.vnode);
    let delete_sql = delete_sql(&payload.vnode);
    let result = txn.execute(&move_sql, &[&payload.owner,
                                          &payload.name])
        .and_then(|_moved_rows| {
            txn.execute(&delete_sql, &[&payload.owner,
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
