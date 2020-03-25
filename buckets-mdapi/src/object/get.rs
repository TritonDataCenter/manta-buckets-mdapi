// Copyright 2020 Joyent, Inc.

use std::vec::Vec;

use serde_json::Error as SerdeError;
use serde_json::Value;
use slog::{debug, error, Logger};

use cueball_postgres_connection::PostgresConnection;
use fast_rpc::protocol::{FastMessage, FastMessageData};

use crate::error::BucketsMdapiError;
use crate::metrics::RegisteredMetrics;
use crate::object::{
    object_not_found, response, to_json, GetObjectPayload, ObjectResponse,
};
use crate::conditional;
use crate::sql;
use crate::types::HandlerResponse;
use crate::util::array_wrap;

pub(crate) fn decode_msg(
    value: &Value,
) -> Result<Vec<GetObjectPayload>, SerdeError> {
    serde_json::from_value::<Vec<GetObjectPayload>>(value.clone())
}

#[allow(clippy::needless_pass_by_value)]
pub(crate) fn action(
    msg_id: u32,
    method: &str,
    metrics: &RegisteredMetrics,
    log: &Logger,
    payload: GetObjectPayload,
    conn: &mut PostgresConnection,
) -> Result<HandlerResponse, String> {
    // Make database request
    do_get(method, &payload, conn, metrics, log)
        .and_then(|maybe_resp| {
            // Handle the successful database response
            debug!(log, "operation successful");
            let value = match maybe_resp {
                Some(resp) => array_wrap(to_json(resp)),
                None => array_wrap(object_not_found()),
            };
            let msg_data = FastMessageData::new(method.into(), value);
            let msg: HandlerResponse =
                FastMessage::data(msg_id, msg_data).into();
            Ok(msg)
        })
        .or_else(|e| {
            if let BucketsMdapiError::PostgresError(_) = &e {
                error!(log, "operation failed"; "error" => e.message());
            }

            /*
             * At this point we've seen some kind of failure processing the request.  It could be
             * that the database has returned an error of some kind, or that there has been a
             * failure in evaluating the conditions of the request.  Either way they are handed
             * back to the application as fast messages.
             */
            let msg_data =
                FastMessageData::new(method.into(), array_wrap(e.into_fast()));
            let msg: HandlerResponse =
                FastMessage::data(msg_id, msg_data).into();
            Ok(msg)
        })
}

fn do_get(
    method: &str,
    payload: &GetObjectPayload,
    conn: &mut PostgresConnection,
    metrics: &RegisteredMetrics,
    log: &Logger,
) -> Result<Option<ObjectResponse>, BucketsMdapiError> {
    let mut txn = (*conn).transaction().map_err(|e| {
        BucketsMdapiError::PostgresError(e.to_string())
    })?;

    let get_sql = sql::get_sql(payload.vnode);

    conditional::request(
        &mut txn,
        &[&payload.owner, &payload.bucket_id, &payload.name],
        payload.vnode,
        &payload.conditions,
        metrics,
        log,
    )
    .and_then(|rows| {
        /*
         * conditional::request will perform the same query as this method, so if it returns
         * Some(rows) then it means the request was both conditional and a success, so we can just
         * return those rows as-is.
         */
        if let Some(r) = rows {
            return Ok(r);
        }

        /*
         * A non-conditional request would not have queried the database yet (and returned
         * Ok(None)), so we run the query directly if not.
         */
        sql::txn_query(
            sql::Method::ObjectGet,
            &mut txn,
            get_sql.as_str(),
            &[&payload.owner, &payload.bucket_id, &payload.name],
            metrics,
            log,
        )
        .map_err(|e| BucketsMdapiError::PostgresError(e.to_string()))
    })
    .and_then(|rows| {
        txn.commit().map_err(|e| BucketsMdapiError::PostgresError(e.to_string()))?;
        Ok(rows)
    })
    .and_then(|rows| response(method, &rows))
}
