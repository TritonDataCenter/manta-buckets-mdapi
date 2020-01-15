// Copyright 2020 Joyent, Inc.

use std::vec::Vec;

use serde_json::Error as SerdeError;
use serde_json::Value;
use slog::{debug, error, Logger};

use cueball_postgres_connection::PostgresConnection;
use rust_fast::protocol::{FastMessage, FastMessageData};

use crate::metrics::RegisteredMetrics;
use crate::object::{
    conditional, object_not_found, response, to_json, GetObjectPayload, ObjectResponse,
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
            // Handle database error response
            error!(log, "operation failed"; "error" => &e);

            // Database errors are returned to as regular Fast messages
            // to be handled by the calling application
            let value = sql::postgres_error(e);
            let msg_data =
                FastMessageData::new(method.into(), array_wrap(value));
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
) -> Result<Option<ObjectResponse>, String> {
    let mut txn = (*conn).transaction().map_err(|e| e.to_string())?;

    conditional(
        &mut txn,
        &payload.owner,
        &payload.bucket_id,
        &payload.name,
        payload.vnode,
        metrics,
        log,
    )
    // XXX
    //
    // this should probably check the response from the conditional call or something, because if
    // the request wasn't conditional then there'd be no rows here, in which case we'd need to get
    // them anyway.  if it's the case that the call isn't conditional, is the txn going to hurt us
    // in any way?
    .and_then(|rows| {
        txn.commit()?;
        Ok(rows)
    })
    .map_err(|e| e.to_string())
    .and_then(|rows| response(method, &rows))
}
