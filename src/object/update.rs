/*
 * Copyright 2019 Joyent, Inc.
 */

use std::io::Error as IOError;
use std::io::ErrorKind as IOErrorKind;
use std::vec::Vec;

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
    object_not_found,
    response
};
use crate::sql;
use crate::util::{
    Hstore,
    array_wrap,
    other_error
};

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateObjectPayload {
    pub owner          : Uuid,
    pub bucket_id      : Uuid,
    pub name           : String,
    pub id             : Uuid,
    pub vnode          : u64,
    pub content_type   : String,
    pub headers        : Hstore,
    pub properties     : Option<Value>,
    pub request_id     : Uuid
}

pub fn handler(msg_id: u32,
                      args: &[Value],
                      mut response: Vec<FastMessage>,
                      pool: &ConnectionPool<PostgresConnection, StaticIpResolver, impl FnMut(&Backend) -> PostgresConnection + Send + 'static>,
                      log: &Logger) -> Result<Vec<FastMessage>, IOError> {
    debug!(log, "handling updateobject function request");

    let arg0 = match &args[0] {
        Value::Object(_) => &args[0],
        _ => return Err(other_error("Expected JSON object"))
    };

    let data_clone = arg0.clone();
    let payload_result: Result<UpdateObjectPayload, _> =
        serde_json::from_value(data_clone);

    let payload = match payload_result {
        Ok(o) => o,
        Err(_) => return Err(other_error("Failed to parse JSON data as payload for updateobject function"))
    };

    debug!(log, "parsed UpdateObjectPayload, req_id: {}", payload.request_id);

    // Make db request and form response
    // let response_msg: Result<FastMessage, IOError> =
    update(payload, pool)
        .and_then(|maybe_resp| {
            let method = String::from("updateobject");
            match maybe_resp {
                Some(resp) => {
                    let value = array_wrap(serde_json::to_value(resp).unwrap());
                    let msg = FastMessage::data(msg_id, FastMessageData::new(method, value));
                    response.push(msg);
                    Ok(response)
                },
                None => {
                    let value = array_wrap(object_not_found());
                    let err_msg = FastMessage::data(msg_id, FastMessageData::new(method, value));
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

fn update(payload: UpdateObjectPayload,
          pool: &ConnectionPool<PostgresConnection, StaticIpResolver, impl FnMut(&Backend) -> PostgresConnection + Send + 'static>)
          -> Result<Option<ObjectResponse>, IOError>
{
    let mut conn = pool.claim().unwrap();
    let mut txn = (*conn).transaction().unwrap();
    let update_sql = update_sql(payload.vnode);

    sql::txn_query(sql::Method::ObjectUpdate, &mut txn, update_sql.as_str(),
                     &[&payload.content_type,
                       &payload.headers,
                       &payload.properties,
                       &payload.owner,
                       &payload.bucket_id,
                       &payload.name])

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

fn update_sql(vnode: u64) -> String {
    ["UPDATE manta_bucket_",
     &vnode.to_string(),
     &".manta_bucket_object \
       SET content_type = $1,
       headers = $2, \
       properties = $3, \
       modified = current_timestamp \
       WHERE owner = $4 \
       AND bucket_id = $5 \
       AND name = $6 \
       RETURNING id, owner, bucket_id, name, created, modified, \
       content_length, content_md5, content_type, headers, \
       sharks, properties"].concat()
}
