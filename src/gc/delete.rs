// Copyright 2019 Joyent, Inc.

use serde_derive::{Deserialize, Serialize};
use serde_json::Error as SerdeError;
use serde_json::{json, Value};
use slog::{debug, error, Logger};

use cueball_postgres_connection::PostgresConnection;
use rust_fast::protocol::{FastMessage, FastMessageData};
use uuid::Uuid;

use crate::sql;
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
        .and_then(|affected_rows| {
            // Handle the successful database response
            debug!(log, "deletebucket operation was successful");
            // This conversion can fail if the implementation of
            // Serialize decides to fail, or if the type
            // contains a map with non-string keys. There is no
            // reason for the former to occur and the latter
            // reason for failure is not a concern here since
            // the type of `affected_rows` is u64.
            let value = serde_json::to_value(affected_rows).unwrap();

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

fn do_delete(payload: &DeleteGarbagePayload, conn: &mut PostgresConnection) -> Result<u64, String> {
    let mut txn = (*conn).transaction().map_err(|e| e.to_string())?;
    let move_sql = insert_delete_table_sql();
    let delete_sql = delete_sql(payload.batch_id);

    sql::txn_execute(
        sql::Method::BucketDeleteMove,
        &mut txn,
        move_sql.as_str(),
        &[],
    )
    .and_then(|_moved_rows| {
        sql::txn_execute(
            sql::Method::BucketDelete,
            &mut txn,
            delete_sql.as_str(),
            &[],
        )
    })
    .and_then(|row_count| {
        txn.commit()?;
        Ok(row_count)
    })
    .map_err(|e| e.to_string())
}

fn insert_delete_table_sql() -> String {
    [
        "INSERT INTO manta_bucket_",
        &".manta_bucket_deleted_bucket \
          (id, owner, name, created) \
          SELECT id, owner, name, created \
          FROM manta_bucket_",
        &".manta_bucket",
    ]
    .concat()
}

fn delete_sql(batch_id: Uuid) -> String {
    [
        "DELETE FROM manta_bucket_",
        &batch_id.to_string(),
        &".manta_bucket",
    ]
    .concat()
}
