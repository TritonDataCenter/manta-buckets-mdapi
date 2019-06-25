/*
 * Copyright 2019 Joyent, Inc.
 */

use std::io::Error as IOError;
use std::io::ErrorKind as IOErrorKind;

use slog::{Logger, debug};
use serde_json::Value;

use cueball::backend::Backend;
use cueball::connection_pool::ConnectionPool;
use cueball_postgres_connection::PostgresConnection;
use cueball_static_resolver::StaticIpResolver;
use rust_fast::protocol::{FastMessage, FastMessageData};

use crate::bucket::{GetBucketPayload, BucketResponse, bucket_not_found, response};
use crate::util::{
    array_wrap,
    other_error
};
use crate::sql;

pub fn handler(msg_id: u32,
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

    debug!(log, "parsed GetBucketPayload, req_id: {}", payload.request_id);

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
                    let value = array_wrap(bucket_not_found());
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

fn get_sql(vnode: u64) -> String {
    ["SELECT id, owner, name, created \
      FROM manta_bucket_",
     &vnode.to_string(),
     &".manta_bucket WHERE owner = $1 \
       AND name = $2"].concat()
}
