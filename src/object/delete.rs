/*
 * Copyright 2019 Joyent, Inc.
 */

use std::io::Error as IOError;
use std::io::ErrorKind as IOErrorKind;
use std::vec::Vec;

use serde_json::{Value, json};
use slog::{Logger, debug};

use cueball::connection_pool::ConnectionPool;
use cueball::backend::Backend;
use cueball_static_resolver::StaticIpResolver;
use cueball_postgres_connection::PostgresConnection;
use rust_fast::protocol::{FastMessage, FastMessageData};

use crate::object::{
    DeleteObjectPayload,
    insert_delete_table_sql,
    object_not_found
};
use crate::sql;
use crate::util::{
    array_wrap,
    other_error
};

pub fn handler(
    msg_id: u32,
    args: &[Value],
    mut response: Vec<FastMessage>,
    pool: &ConnectionPool<PostgresConnection, StaticIpResolver, impl FnMut(&Backend) -> PostgresConnection + Send + 'static>,
    log: &Logger
) -> Result<Vec<FastMessage>, IOError> {
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

    debug!(log, "parsed DeleteObjectPayload, req_id: {}", payload.request_id);

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
                let value = array_wrap(object_not_found());
                let err_msg = FastMessage::data(msg_id, FastMessageData::new(method, value));
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

fn delete(payload: DeleteObjectPayload,
          pool: &ConnectionPool<PostgresConnection, StaticIpResolver, impl FnMut(&Backend) -> PostgresConnection + Send + 'static>)
          -> Result<u64, IOError>
{
    let mut conn = pool.claim().unwrap();
    let mut txn = (*conn).transaction().unwrap();
    let move_sql = insert_delete_table_sql(payload.vnode);
    let delete_sql = delete_sql(payload.vnode);

    sql::txn_execute(sql::Method::ObjectDeleteMove, &mut txn, move_sql.as_str(),
                     &[&payload.owner,
                     &payload.bucket_id,
                     &payload.name])
        .and_then(|_moved_rows| {
            sql::txn_execute(sql::Method::ObjectDelete, &mut txn, delete_sql.as_str(),
                             &[&payload.owner,
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


fn delete_sql(vnode: u64) -> String {
    ["DELETE FROM manta_bucket_",
     &vnode.to_string(),
     &".manta_bucket_object \
       WHERE owner = $1 \
       AND bucket_id = $2 \
       AND name = $3"].concat()
}
