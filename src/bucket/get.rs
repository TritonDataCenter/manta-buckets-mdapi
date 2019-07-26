// Copyright 2019 Joyent, Inc.

use slog::{Logger, debug, error, warn};
use serde_json::{Value, json};

use cueball_postgres_connection::PostgresConnection;
use rust_fast::protocol::{FastMessage, FastMessageData};

use crate::bucket::{
    GetBucketPayload,
    BucketResponse,
    bucket_not_found,
    response,
    to_json
};
use crate::util::{
    HandlerError,
    HandlerResponse,
    array_wrap,
    other_error
};
use crate::sql;

const METHOD: &str = "getbucket";

pub(crate) fn handler(
    msg_id: u32,
    data: &Value,
    mut conn: &mut PostgresConnection,
    log: &Logger
) -> Result<HandlerResponse, HandlerError>
{
    debug!(log, "handling {} function request", &METHOD);

    serde_json::from_value::<Vec<GetBucketPayload>>(data.clone())
        .map_err(|e| e.to_string())
        .and_then(|mut arr| {
            // Remove outer JSON array required by Fast
            if !arr.is_empty() {
                Ok(arr.remove(0))
            } else {
                let err_msg = "Failed to parse JSON data as payload for \
                               getbucket function";
                warn!(log, "{}: {}", err_msg, data);
                Err(err_msg.to_string())
            }
        })
        .and_then(|payload| {
            // Make database request
            let req_id = payload.request_id;
            debug!(log, "parsed GetBucketPayload, req_id: {}", &req_id);

            get(payload, &mut conn)
                .and_then(|maybe_resp| {
                    // Handle the successful database response
                    debug!(log, "{} operation was successful, req_id: {}", &METHOD, &req_id);
                    let value =
                        match maybe_resp {
                            Some(resp) => to_json(resp),
                            None => bucket_not_found()
                        };
                    let msg_data =
                        FastMessageData::new(METHOD.into(), array_wrap(value));
                    let msg: HandlerResponse =
                        FastMessage::data(msg_id, msg_data).into();
                    Ok(msg)
                })
                .or_else(|e| {
                    // Handle database error response
                    error!(log, "{} operation failed: {}, req_id: {}", &METHOD, &e, &req_id);

                    // Database errors are returned to as regular Fast messages
                    // to be handled by the calling application
                    let value = array_wrap(json!({
                        "name": "PostgresError",
                        "message": e
                    }));

                    let msg_data = FastMessageData::new(METHOD.into(), value);
                    let msg: HandlerResponse =
                        FastMessage::data(msg_id, msg_data).into();
                    Ok(msg)
                })
        })
        .map_err(|e| HandlerError::IO(other_error(&e)))
}

fn get(
    payload: GetBucketPayload,
    mut conn: &mut PostgresConnection
) -> Result<Option<BucketResponse>, String>
{
    let sql = get_sql(payload.vnode);

    sql::query(sql::Method::BucketGet, &mut conn, sql.as_str(),
               &[&payload.owner,
                 &payload.name])
        .map_err(|e| e.to_string())
        .and_then(|rows| {
            response(METHOD, rows)
        })
}

fn get_sql(
    vnode: u64
) -> String
{
    ["SELECT id, owner, name, created \
      FROM manta_bucket_",
     &vnode.to_string(),
     &".manta_bucket WHERE owner = $1 \
       AND name = $2"].concat()
}
