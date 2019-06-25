/*
 * Copyright 2019 Joyent, Inc.
 */

use std::io::Error as IOError;
use std::io::ErrorKind as IOErrorKind;
use std::vec::Vec;

use serde_json::Value;
use slog::{Logger, debug};

use cueball::connection_pool::ConnectionPool;
use cueball::backend::Backend;
use cueball_static_resolver::StaticIpResolver;
use cueball_postgres_connection::PostgresConnection;
use rust_fast::protocol::{FastMessage, FastMessageData};

use crate::object::{
    GetObjectPayload,
    ObjectResponse,
    object_not_found,
    response
};
use crate::sql;
use crate::util::{
    array_wrap,
    other_error
};

pub fn handler(msg_id: u32,
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

    debug!(log, "parsed GetObjectPayload, req_id: {}", payload.request_id);
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

fn get(payload: GetObjectPayload,
       pool: &ConnectionPool<PostgresConnection, StaticIpResolver, impl FnMut(&Backend) -> PostgresConnection + Send + 'static>)
       -> Result<Option<ObjectResponse>, IOError>
{
    let mut conn = pool.claim().unwrap();
    let sql = get_sql(payload.vnode);

    sql::query(sql::Method::ObjectGet, &mut conn, sql.as_str(),
               &[&payload.owner,
               &payload.bucket_id,
               &payload.name])
        .map_err(|e| {
            let pg_err = format!("{}", e);
            IOError::new(IOErrorKind::Other, pg_err)
        })
        .and_then(response)
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
