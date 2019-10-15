// Copyright 2019 Joyent, Inc.

use serde_json::Error as SerdeError;
use serde_json::{json, Value};
use slog::{debug, error, Logger};

use cueball_postgres_connection::PostgresConnection;
use rust_fast::protocol::{FastMessage, FastMessageData};

use crate::bucket::{bucket_not_found, response, to_json, BucketResponse, GetBucketPayload};
use crate::sql;
use crate::types::HandlerResponse;
use crate::util::array_wrap;

pub(crate) fn decode_msg(value: &Value) -> Result<Vec<GetBucketPayload>, SerdeError> {
    serde_json::from_value::<Vec<GetBucketPayload>>(value.clone())
}

pub(crate) fn action(
    msg_id: u32,
    method: &str,
    log: &Logger,
    payload: GetBucketPayload,
    conn: &mut PostgresConnection,
) -> Result<HandlerResponse, String> {
    // Make database request
    do_get(method, payload, conn)
        .and_then(|maybe_resp| {
            // Handle the successful database response
            debug!(log, "{} operation was successful", &method);
            let value = match maybe_resp {
                Some(resp) => to_json(resp),
                None => bucket_not_found(),
            };
            let msg_data = FastMessageData::new(method.into(), array_wrap(value));
            let msg: HandlerResponse = FastMessage::data(msg_id, msg_data).into();
            Ok(msg)
        })
        .or_else(|e| {
            // Handle database error response
            error!(log, "{} operation failed: {}", &method, &e);

            // Database errors are returned to as regular Fast messages
            // to be handled by the calling application
            let value = array_wrap(json!({
                "name": "PostgresError",
                "message": e
            }));

            let msg_data = FastMessageData::new(method.into(), value);
            let msg: HandlerResponse = FastMessage::data(msg_id, msg_data).into();
            Ok(msg)
        })
}

fn do_get(
    method: &str,
    payload: GetBucketPayload,
    mut conn: &mut PostgresConnection,
) -> Result<Option<BucketResponse>, String> {
    let sql = get_sql(payload.vnode);

    sql::query(
        sql::Method::BucketGet,
        &mut conn,
        sql.as_str(),
        &[&payload.owner, &payload.name],
    )
    .map_err(|e| e.to_string())
    .and_then(|rows| response(method, rows))
}

fn get_sql(vnode: u64) -> String {
    [
        "SELECT id, owner, name, created \
         FROM manta_bucket_",
        &vnode.to_string(),
        &".manta_bucket WHERE owner = $1 \
          AND name = $2",
    ]
    .concat()
}
