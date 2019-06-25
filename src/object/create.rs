/*
 * Copyright 2019 Joyent, Inc.
 */

use std::io::Error as IOError;
use std::io::ErrorKind as IOErrorKind;
use std::vec::Vec;

use base64;
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;
use slog::{Logger, debug};
use uuid::Uuid;

use cueball::connection_pool::ConnectionPool;
use cueball::backend::Backend;
use cueball_static_resolver::StaticIpResolver;
use cueball_postgres_connection::PostgresConnection;
use rust_fast::protocol::{FastMessage, FastMessageData};

use crate::object::{
    ObjectResponse,
    StorageNodeIdentifier,
    insert_delete_table_sql,
    response
};
use crate::sql;
use crate::util::{
    Hstore,
    array_wrap,
    other_error
};

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateObjectPayload {
    pub owner          : Uuid,
    pub bucket_id      : Uuid,
    pub name           : String,
    pub id             : Uuid,
    pub vnode          : u64,
    pub content_length : i64,
    pub content_md5    : String,
    pub content_type   : String,
    pub headers        : Hstore,
    pub sharks         : Vec<StorageNodeIdentifier>,
    pub properties     : Option<Value>,
    pub request_id     : Uuid
}

pub fn handler(msg_id: u32,
                   args: &[Value],
                   mut response: Vec<FastMessage>,
                   pool: &ConnectionPool<PostgresConnection, StaticIpResolver, impl FnMut(&Backend) -> PostgresConnection + Send + 'static>,
                   log: &Logger) -> Result<Vec<FastMessage>, IOError> {
    debug!(log, "handling createobject function request");

    let arg0 = match &args[0] {
        Value::Object(_) => &args[0],
        _ => return Err(other_error("Expected JSON object"))
    };

    let data_clone = arg0.clone();
    let payload_result: Result<CreateObjectPayload, _> =
        serde_json::from_value(data_clone);

    let payload = match payload_result {
        Ok(o) => o,
        Err(_) => return Err(other_error("Failed to parse JSON data as payload for createobject function"))
    };

    debug!(log, "parsed CreateObjectPayload, req_id: {}", payload.request_id);
    // Make db request and form response
    create(payload, pool)
        .and_then(|resp| {
            let method = String::from("createobject");
            let value = array_wrap(serde_json::to_value(resp).unwrap());
            let msg = FastMessage::data(msg_id, FastMessageData::new(method, value));
            response.push(msg);
            Ok(response)
        })
        //TODO: Proper error handling
        .map_err(|_e| other_error("postgres error"))
}

fn create(payload: CreateObjectPayload,
       pool: &ConnectionPool<PostgresConnection, StaticIpResolver, impl FnMut(&Backend) -> PostgresConnection + Send + 'static>)
       -> Result<Option<ObjectResponse>, IOError>
{
    let mut conn = pool.claim().unwrap();
    let mut txn = (*conn).transaction().unwrap();
    let create_sql = create_sql(payload.vnode);
    let move_sql = insert_delete_table_sql(payload.vnode);
    let content_md5_bytes = base64::decode(&payload.content_md5).unwrap();

    sql::txn_execute(sql::Method::ObjectCreateMove, &mut txn, move_sql.as_str(),
                     &[&payload.owner,
                     &payload.bucket_id,
                     &payload.name])
        .and_then(|_moved_rows| {
            sql::txn_query(sql::Method::ObjectCreate, &mut txn, create_sql.as_str(),
                           &[&payload.id,
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

fn create_sql(vnode: u64) -> String {
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
