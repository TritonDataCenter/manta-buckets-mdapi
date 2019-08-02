// Copyright 2019 Joyent, Inc.

use std::vec::Vec;

use serde_json::{json, Value};
use slog::{debug, error, warn, Logger};

use cueball_postgres_connection::PostgresConnection;
use rust_fast::protocol::{FastMessage, FastMessageData};

use crate::object::{insert_delete_table_sql, object_not_found, DeleteObjectPayload};
use crate::sql;
use crate::util::{array_wrap, other_error, HandlerError, HandlerResponse};

const METHOD: &str = "deleteobject";

pub(crate) fn handler(
    msg_id: u32,
    data: &Value,
    mut conn: &mut PostgresConnection,
    log: &Logger,
) -> Result<HandlerResponse, HandlerError> {
    debug!(log, "handling {} function request", &METHOD);

    serde_json::from_value::<Vec<DeleteObjectPayload>>(data.clone())
        .map_err(|e| e.to_string())
        .and_then(|mut arr| {
            // Remove outer JSON array required by Fast
            if !arr.is_empty() {
                Ok(arr.remove(0))
            } else {
                let err_msg = "Failed to parse JSON data as payload for \
                               deleteobject function";
                warn!(log, "{}: {}", err_msg, data);
                Err(err_msg.to_string())
            }
        })
        .and_then(|payload| {
            // Make database request
            let req_id = payload.request_id;
            debug!(log, "parsed DeleteObjectPayload, req_id: {}", &req_id);

            delete(payload, &mut conn)
                .and_then(|affected_rows| {
                    // Handle the successful database response
                    debug!(
                        log,
                        "{} operation was successful, req_id: {}", &METHOD, &req_id
                    );
                    let value = if affected_rows > 0 {
                        // This conversion can fail if the implementation of
                        // Serialize decides to fail, or if the type
                        // contains a map with non-string keys. There is no
                        // reason for the former to occur and the latter
                        // reason for failure is not a concern here since
                        // the type of `affected_rows` is u64.
                        serde_json::to_value(affected_rows).unwrap()
                    } else {
                        object_not_found()
                    };

                    let msg_data = FastMessageData::new(METHOD.into(), array_wrap(value));
                    let msg: HandlerResponse = FastMessage::data(msg_id, msg_data).into();
                    Ok(msg)
                })
                .or_else(|e| {
                    // Handle database error response
                    error!(
                        log,
                        "{} operation failed: {}, req_id: {}", &METHOD, &e, &req_id
                    );

                    // Database errors are returned to as regular Fast messages
                    // to be handled by the calling application
                    let value = array_wrap(json!({
                        "name": "PostgresError",
                        "message": e
                    }));

                    let msg_data = FastMessageData::new(METHOD.into(), value);
                    let msg: HandlerResponse = FastMessage::data(msg_id, msg_data).into();
                    Ok(msg)
                })
        })
        .map_err(|e| HandlerError::IO(other_error(&e)))
}

fn delete(payload: DeleteObjectPayload, conn: &mut PostgresConnection) -> Result<u64, String> {
    let mut txn = (*conn).transaction().map_err(|e| e.to_string())?;
    let move_sql = insert_delete_table_sql(payload.vnode);
    let delete_sql = delete_sql(payload.vnode);

    sql::txn_execute(
        sql::Method::ObjectDeleteMove,
        &mut txn,
        move_sql.as_str(),
        &[&payload.owner, &payload.bucket_id, &payload.name],
    )
    .and_then(|_moved_rows| {
        sql::txn_execute(
            sql::Method::ObjectDelete,
            &mut txn,
            delete_sql.as_str(),
            &[&payload.owner, &payload.bucket_id, &payload.name],
        )
    })
    .and_then(|row_count| {
        txn.commit()?;
        Ok(row_count)
    })
    .map_err(|e| e.to_string())
}

fn delete_sql(vnode: u64) -> String {
    [
        "DELETE FROM manta_bucket_",
        &vnode.to_string(),
        &".manta_bucket_object \
          WHERE owner = $1 \
          AND bucket_id = $2 \
          AND name = $3",
    ]
    .concat()
}
