// Copyright 2019 Joyent, Inc.

use std::vec::Vec;

use serde_json::Error as SerdeError;
use serde_json::Value;
use slog::{debug, error, Logger};

use cueball_postgres_connection::PostgresConnection;
use rust_fast::protocol::{FastMessage, FastMessageData};

use crate::object::{
    insert_delete_table_sql, object_not_found, DeleteObjectPayload, DeleteObjectResponse,
};
use crate::sql;
use crate::types::HandlerResponse;
use crate::util::array_wrap;

pub(crate) fn decode_msg(value: &Value) -> Result<Vec<DeleteObjectPayload>, SerdeError> {
    serde_json::from_value::<Vec<DeleteObjectPayload>>(value.clone())
}

#[allow(clippy::needless_pass_by_value)]
pub(crate) fn action(
    msg_id: u32,
    method: &str,
    log: &Logger,
    payload: DeleteObjectPayload,
    conn: &mut PostgresConnection,
) -> Result<HandlerResponse, String> {
    // Make database request
    do_delete(&payload, conn, log)
        .and_then(|deleted_objects| {
            // Handle the successful database response
            debug!(log, "operation successful");

            let value = if deleted_objects.is_empty() {
                object_not_found()
            } else {
                // This is not expected to fail. As long as DeleteObjectResponse can be
                // serialized, so a vector of DeleteObjectResponse should be.
                serde_json::to_value(deleted_objects).unwrap()
            };

            let msg_data = FastMessageData::new(method.into(), array_wrap(value));
            let msg: HandlerResponse = FastMessage::data(msg_id, msg_data).into();
            Ok(msg)
        })
        .or_else(|e| {
            // Handle database error response
            error!(log, "operation failed"; "error" => &e);

            // Database errors are returned to as regular Fast messages
            // to be handled by the calling application
            let value = sql::postgres_error(e);
            let msg_data = FastMessageData::new(method.into(), array_wrap(value));
            let msg: HandlerResponse = FastMessage::data(msg_id, msg_data).into();
            Ok(msg)
        })
}

fn do_delete(
    payload: &DeleteObjectPayload,
    conn: &mut PostgresConnection,
    log: &Logger,
) -> Result<Vec<DeleteObjectResponse>, String> {
    let mut txn = (*conn).transaction().map_err(|e| e.to_string())?;
    let move_sql = insert_delete_table_sql(payload.vnode);
    let delete_sql = delete_sql(payload.vnode);

    sql::txn_execute(
        sql::Method::ObjectDeleteMove,
        &mut txn,
        move_sql.as_str(),
        &[&payload.owner, &payload.bucket_id, &payload.name],
        &log,
    )
    .and_then(|_moved_rows| {
        sql::txn_query(
            sql::Method::ObjectDelete,
            &mut txn,
            delete_sql.as_str(),
            &[&payload.owner, &payload.bucket_id, &payload.name],
            &log,
        )
    })
    .and_then(|deleted_objects| {
        let mut objs = vec![];
        for row in deleted_objects {
            /*
             * As of now, there is no constraint in the database to guarantee that
             * 'content_length' is not null, as a result of that we need to be cautious
             * while tying to get an integer out of it. Also, 'shark_count' could be
             * null, the reason is that array_length() returns 'null' if the array is
             * empty instead of returning '0'. Yeah, this is weird!
             */
            let content_length = row.try_get("content_length")?;
            let shark_count = row.try_get("shark_count").unwrap_or(0);

            let obj = DeleteObjectResponse {
                id: row.get("id"),
                owner: row.get("owner"),
                bucket_id: row.get("bucket_id"),
                name: row.get("name"),
                content_length,
                shark_count,
            };

            objs.push(obj);
        }

        txn.commit()?;
        Ok(objs)
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
        "RETURNING id, \
         owner, \
         bucket_id, \
         name, \
         content_length, \
         array_length(sharks, 1) as shark_count",
    ]
    .concat()
}
