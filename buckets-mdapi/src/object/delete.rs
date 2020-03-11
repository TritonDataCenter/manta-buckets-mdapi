// Copyright 2020 Joyent, Inc.

use std::vec::Vec;

use serde_json::Error as SerdeError;
use serde_json::Value;
use slog::{debug, error, Logger};

use cueball_postgres_connection::PostgresConnection;
use fast_rpc::protocol::{FastMessage, FastMessageData};

use crate::metrics::RegisteredMetrics;
use crate::object::{
    insert_delete_table_sql, object_not_found, DeleteObjectPayload,
    DeleteObjectResponse,
};
use crate::conditional;
use crate::sql;
use crate::types::HandlerResponse;
use crate::util::array_wrap;

pub(crate) fn decode_msg(
    value: &Value,
) -> Result<Vec<DeleteObjectPayload>, SerdeError> {
    serde_json::from_value::<Vec<DeleteObjectPayload>>(value.clone())
}

#[allow(clippy::needless_pass_by_value)]
pub(crate) fn action(
    msg_id: u32,
    method: &str,
    metrics: &RegisteredMetrics,
    log: &Logger,
    payload: DeleteObjectPayload,
    conn: &mut PostgresConnection,
) -> Result<HandlerResponse, String> {
    // Make database request
    do_delete(&payload, conn, metrics, log)
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

            let msg_data =
                FastMessageData::new(method.into(), array_wrap(value));
            let msg: HandlerResponse =
                FastMessage::data(msg_id, msg_data).into();
            Ok(msg)
        })
        .or_else(|e| {
            if e["name"] == "PostgresError" {
                error!(log, "operation failed"; "error" => &e.to_string());
            }

            let msg_data =
                FastMessageData::new(method.into(), array_wrap(e));
            let msg: HandlerResponse =
                FastMessage::data(msg_id, msg_data).into();
            Ok(msg)
        })
}

fn do_delete(
    payload: &DeleteObjectPayload,
    conn: &mut PostgresConnection,
    metrics: &RegisteredMetrics,
    log: &Logger,
) -> Result<Vec<DeleteObjectResponse>, Value> {
    let mut txn = (*conn).transaction().map_err(|e| e.to_string())?;
    let move_sql = insert_delete_table_sql(payload.vnode);
    let delete_sql = delete_sql(payload.vnode);

    conditional::request(
        &mut txn,
        &[&payload.owner, &payload.bucket_id, &payload.name],
        payload.vnode,
        &payload.conditions,
        metrics,
        log,
    )
    .and_then(|_rows| {
        sql::txn_execute(
            sql::Method::ObjectDeleteMove,
            &mut txn,
            move_sql.as_str(),
            &[&payload.owner, &payload.bucket_id, &payload.name],
            metrics,
            &log,
        )
        .and_then(|_moved_rows| {
            sql::txn_query(
                sql::Method::ObjectDelete,
                &mut txn,
                delete_sql.as_str(),
                &[&payload.owner, &payload.bucket_id, &payload.name],
                metrics,
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

            Ok(objs)
        })
        .map_err(|e| { sql::postgres_error(e.to_string()) })
    })
    .and_then(|rows| {
        txn.commit().map_err(|e| { sql::postgres_error(e.to_string()) })?;
        Ok(rows)
    })
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
