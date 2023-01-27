// Copyright 2020 Joyent, Inc.
// Copyright 2023 MNX Cloud, Inc.

use std::vec::Vec;

use serde_json::Error as SerdeError;
use serde_json::Value;
use slog::{debug, error, Logger};

use cueball_postgres_connection::PostgresConnection;
use fast_rpc::protocol::{FastMessage, FastMessageData};

use crate::error::BucketsMdapiError;
use crate::metrics::RegisteredMetrics;
use crate::object::{
    get_sql, response, to_json, GetObjectPayload, ObjectResponse,
};
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
        .and_then(|object_resp| {
            // Handle the successful database response
            debug!(log, "operation successful");
            let value = array_wrap(to_json(object_resp));
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
             * At this point we've seen some kind of failure processing the
             * request.  It could be that the database has returned an error of
             * some kind, or that there has been a failure in evaluating the
             * conditions of the request.  Either way they are handed back to
             * the application as fast messages.
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
    mut conn: &mut PostgresConnection,
    metrics: &RegisteredMetrics,
    log: &Logger,
) -> Result<ObjectResponse, BucketsMdapiError> {
    let sql = get_sql(payload.vnode);

    /*
     * This request is conditional, but the contitional::request method will run
     * the same SQL as this, so we can save a roundtrip by running the query
     * ourselves and just pass the result to the conditional check.
     */
    sql::query(
        sql::Method::ObjectGet,
        &mut conn,
        sql.as_str(),
        &[&payload.owner, &payload.bucket_id, &payload.name],
        metrics,
        log,
    )
    .map_err(|e| BucketsMdapiError::PostgresError(e.to_string()))
    .and_then(|rows| response(method, &rows))
    .and_then(|maybe_resp| match maybe_resp {
        None => Err(BucketsMdapiError::ObjectNotFound),
        Some(object) => {
            payload.conditions.check(Some(&object))?;

            Ok(object)
        }
    })
}
