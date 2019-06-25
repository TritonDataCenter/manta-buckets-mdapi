/*
 * Copyright 2019 Joyent, Inc.
 */

use std::io::Error as IOError;
use std::io::ErrorKind as IOErrorKind;

use serde_derive::{Deserialize, Serialize};
use serde_json::Value;
use slog::{Logger, debug};
use uuid::Uuid;

use cueball::backend::Backend;
use cueball::connection_pool::ConnectionPool;
use cueball_postgres_connection::PostgresConnection;
use cueball_static_resolver::StaticIpResolver;
use rust_fast::protocol::{FastMessage, FastMessageData};

use crate::bucket::{
    BucketResponse,
    bucket_already_exists,
    response
};
use crate::sql;
use crate::util::{
    array_wrap,
    other_error
};

#[derive(Serialize, Deserialize)]
pub struct CreateBucketPayload {
    pub owner      : Uuid,
    pub name       : String,
    pub vnode      : u64,
    pub request_id : Uuid
}

pub fn handler(msg_id: u32,
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

    debug!(log, "parsed CreateBucketPayload, req_id: {}", payload.request_id);

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
                    let value = array_wrap(bucket_already_exists());
                    let err_msg = FastMessage::data(msg_id, FastMessageData::new(method, value));
                    response.push(err_msg);
                    Ok(response)
                }
            }
        })
        //TODO: Proper error handling
        .map_err(|_e| other_error("postgres error"))
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

fn create_sql(vnode: u64) -> String {
    ["INSERT INTO manta_bucket_",
     &vnode.to_string(),
     &".manta_bucket \
       (id, owner, name) \
       VALUES ($1, $2, $3) \
       ON CONFLICT DO NOTHING \
       RETURNING id, owner, name, created"].concat()
}
