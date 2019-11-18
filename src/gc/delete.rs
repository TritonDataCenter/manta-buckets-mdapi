// Copyright 2019 Joyent, Inc.

use serde_derive::{Deserialize, Serialize};
use serde_json::Error as SerdeError;
use serde_json::{json, Value};
use slog::{debug, error, Logger};

use cueball_postgres_connection::PostgresConnection;
use rust_fast::protocol::{FastMessage, FastMessageData};
use uuid::Uuid;

use crate::types::{HasRequestId, HandlerResponse};
use crate::util::array_wrap;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct DeleteGarbagePayload {
    pub request_id: Uuid,
    pub batch_id: Uuid,
}

impl HasRequestId for DeleteGarbagePayload {
    fn request_id(&self) -> Uuid {
        self.request_id
    }
}

pub(crate) fn decode_msg(value: &Value) -> Result<Vec<DeleteGarbagePayload>, SerdeError> {
    serde_json::from_value::<Vec<DeleteGarbagePayload>>(value.clone())
}

#[allow(clippy::needless_pass_by_value)]
pub(crate) fn action(
    msg_id: u32,
    method: &str,
    log: &Logger,
    payload: DeleteGarbagePayload,
    conn: &mut PostgresConnection,
) -> Result<HandlerResponse, String> {
    // Make database request
    do_delete(&payload, conn)
        .and_then(|_affected_rows| {
            // Handle the successful database response
            debug!(log, "{} operation was successful", &method);

            let value = json!("ok");

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

fn do_delete(_payload: &DeleteGarbagePayload, _conn: &mut PostgresConnection) -> Result<(), String> {
    // TODO: Implement garbage batch deletion logic
    Ok(())
}
