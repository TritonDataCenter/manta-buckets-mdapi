// Copyright 2019 Joyent, Inc.

use std::vec::Vec;

use serde_json::{json, Value};
use slog::{debug, error, warn, Logger};

use cueball_postgres_connection::PostgresConnection;
use rust_fast::protocol::{FastMessage, FastMessageData};

use crate::object::{object_not_found, response, to_json, GetObjectPayload, ObjectResponse};
use crate::sql;
use crate::util::{array_wrap, other_error, HandlerError, HandlerResponse};

const METHOD: &str = "getobject";

pub(crate) fn handler(
    msg_id: u32,
    data: &Value,
    mut conn: &mut PostgresConnection,
    log: &Logger,
) -> Result<HandlerResponse, HandlerError> {
    debug!(log, "handling getobject function request");

    serde_json::from_value::<Vec<GetObjectPayload>>(data.clone())
        .map_err(|e| e.to_string())
        .and_then(|mut arr| {
            // Remove outer JSON array required by Fast
            if !arr.is_empty() {
                Ok(arr.remove(0))
            } else {
                let err_msg = "Failed to parse JSON data as payload for \
                               getobject function";
                warn!(log, "{}: {}", err_msg, data);
                Err(err_msg.to_string())
            }
        })
        .and_then(|payload| {
            // Make database request
            let req_id = payload.request_id;
            debug!(log, "parsed GetObjectPayload, req_id: {}", &req_id);

            get(payload, &mut conn)
                .and_then(|maybe_resp| {
                    // Handle the successful database response
                    debug!(
                        log,
                        "getobject operation was successful, req_id: {}", &req_id
                    );
                    let value = match maybe_resp {
                        Some(resp) => array_wrap(to_json(resp)),
                        None => array_wrap(object_not_found()),
                    };
                    let msg_data = FastMessageData::new(METHOD.into(), value);
                    let msg: HandlerResponse = FastMessage::data(msg_id, msg_data).into();
                    Ok(msg)
                })
                .or_else(|e| {
                    // Handle database error response
                    error!(
                        log,
                        "getobject operation failed: {}, req_id: {}", &e, &req_id
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

fn get(
    payload: GetObjectPayload,
    mut conn: &mut PostgresConnection,
) -> Result<Option<ObjectResponse>, String> {
    let sql = get_sql(payload.vnode);

    sql::query(
        sql::Method::ObjectGet,
        &mut conn,
        sql.as_str(),
        &[&payload.owner, &payload.bucket_id, &payload.name],
    )
    .map_err(|e| e.to_string())
    .and_then(|rows| response(METHOD, rows))
}

fn get_sql(vnode: u64) -> String {
    [
        "SELECT id, owner, bucket_id, name, created, modified, content_length, \
         content_md5, content_type, headers, sharks, properties \
         FROM manta_bucket_",
        &vnode.to_string(),
        &".manta_bucket_object WHERE owner = $1 \
          AND bucket_id = $2 \
          AND name = $3",
    ]
    .concat()
}
