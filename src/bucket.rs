use std::io::Error as IOError;
use std::io::ErrorKind as IOErrorKind;

use chrono;
use r2d2::Pool;
use postgres::Result as PostgresResult;
use postgres::rows::Rows;
use r2d2_postgres::PostgresConnectionManager;
use serde_json::Value;
use slog::Logger;
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

#[derive(Serialize, Deserialize)]
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

fn array_wrap(v: Value) -> Value {
    Value::Array(vec![v])
}

pub fn get_handler(msg_id: u32,
                   args: &Vec<Value>,
                   mut response: Vec<FastMessage>,
                   pool: &Pool<PostgresConnectionManager>,
                   log: &Logger) -> Result<Vec<FastMessage>, IOError> {
    debug!(log, "handling getbucket function request");
    let arg0 = &args[0];
    match arg0 {
        Value::Object(_) => {
            let data_clone = arg0.clone();
            let payload_result: Result<GetBucketPayload, _> =
                serde_json::from_value(data_clone);
            match payload_result {
                Ok(payload) => {
                    // Make db request and form response
                    get(payload, pool)
                        .and_then(|resp| {
                            // let name = msg.data.m.name.clone();
                            let method = String::from("getbucket");
                            let value = array_wrap(serde_json::to_value(resp).unwrap());
                            let msg = FastMessage::data(msg_id, FastMessageData::new(method, value));
                            response.push(msg);
                            Ok(response)
                        })
                        //TODO: Proper error handling
                        .map_err(|e| {
                            println!("Error: {}", e);
                            other_error("postgres error")
                        })
                },
                Err(_) =>
                    Err(other_error("Failed to parse JSON data as payload for getbucket function"))
            }
        }
        _ => Err(other_error("Expected JSON object"))
    }
}


pub fn put_handler(msg_id: u32,
                   args: &Vec<Value>,
                   mut response: Vec<FastMessage>,
                   pool: &Pool<PostgresConnectionManager>,
                   log: &Logger) -> Result<Vec<FastMessage>, IOError> {
    debug!(log, "handling putbucket function request");
    let arg0 = &args[0];
    match arg0 {
        Value::Object(_) => {
            let data_clone = arg0.clone();
            let payload_result: Result<PutBucketPayload, _> =
                serde_json::from_value(data_clone);
            match payload_result {
                Ok(payload) => {
                    // Make db request and form response
                    put(payload, pool)
                        .and_then(|resp| {
                            let method = String::from("putbucket");
                            let value = array_wrap(serde_json::to_value(resp).unwrap());
                            let msg = FastMessage::data(msg_id, FastMessageData::new(method, value));
                            response.push(msg);
                            Ok(response)
                        })
                        //TODO: Proper error handling
                        .map_err(|_e| other_error("postgres error"))
                },
                Err(_) =>
                    Err(other_error("Failed to parse JSON data as payload for putbucket function"))
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
    debug!(log, "handling putbucket function request");
    let arg0 = &args[0];
    match arg0 {
        Value::Object(_) => {
            let data_clone = arg0.clone();
            let payload_result: Result<DeleteBucketPayload, _> =
                serde_json::from_value(data_clone);
            match payload_result {
                Ok(payload) => {
                    // Make db request and form response
                    delete(payload, pool)
                        .and_then(|resp| {
                            let method = String::from("deletebucket");
                            let value = array_wrap(serde_json::to_value(resp).unwrap());
                            let msg = FastMessage::data(msg_id, FastMessageData::new(method, value));
                            response.push(msg);
                            Ok(response)
                        })
                        //TODO: Proper error handling
                        .map_err(|_e| other_error("postgres error"))
                },
                Err(_) =>
                    Err(other_error("Failed to parse JSON data as payload for deletebucket function"))
            }
        }
        _ => Err(other_error("Expected JSON object"))
    }
}


fn other_error(msg: &str) -> IOError {
    IOError::new(IOErrorKind::Other, String::from(msg))
}


fn response(rows: Rows) -> PostgresResult<BucketResponse> {
    if rows.len() == 1 {
        let row = rows.get(0);
        //TODO: Valdate # of cols
        let resp = BucketResponse {
            id             : row.get(0),
            owner          : row.get(1),
            name           : row.get(2),
            created        : row.get(3)
        };
        Ok(resp)
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
       RETURNING id, owner, name, created"].concat()
}

fn get(payload: GetBucketPayload, pool: &Pool<PostgresConnectionManager>)
           -> PostgresResult<BucketResponse>
{
    let conn = pool.get().unwrap();
    let sql = get_sql(&payload.vnode);
    conn.query(&sql, &[&payload.owner,
                       &payload.name])
        .and_then(|rows| response(rows))
        .map_err(|e| e)
}


fn put(payload: PutBucketPayload, pool: &Pool<PostgresConnectionManager>)
           -> PostgresResult<BucketResponse>
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
