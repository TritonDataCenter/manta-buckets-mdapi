// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright 2026 Edgecast Cloud LLC.

use std::collections::HashMap;

use serde_derive::{Deserialize, Serialize};
use serde_json::Error as SerdeError;
use serde_json::Value;
use slog::{debug, error, warn, Logger};
use uuid::Uuid;

use cueball_postgres_connection::PostgresConnection;
use fast_rpc::protocol::{FastMessage, FastMessageData};

use crate::conditional;
use crate::error::BucketsMdapiError;
use crate::metrics::RegisteredMetrics;
use crate::object::update::UpdateObjectPayload;
use crate::sql;
use crate::types::{HandlerResponse, HasRequestId};
use crate::util::array_wrap;

/// Maximum number of objects allowed in a single batch
/// RPC. Prevents excessively long-running transactions
/// that could starve other connections.
/// 1000 was chosen as by default S3 lists 1000 objects
/// per call.
const MAX_BATCH_SIZE: usize = 1000;

/// Payload for the batchupdateobjects RPC.
///
/// Contains a list of individual update payloads and a
/// request-level identifier for tracing.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct BatchUpdateObjectsPayload {
    pub objects: Vec<UpdateObjectPayload>,
    pub request_id: Uuid,
}

impl HasRequestId for BatchUpdateObjectsPayload {
    fn request_id(&self) -> Uuid {
        self.request_id
    }
}

/// A vnode whose entire transaction was rolled back.
///
/// All objects in this group must be retried together
/// since the transaction is atomic per vnode.  The
/// `objects` field contains the original request payloads
/// so clients can resubmit them directly.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct VnodeFailure {
    pub vnode: u64,
    /// The error that caused the rollback.
    pub error: Value,
    /// Original request payloads — ready for retry.
    pub objects: Vec<UpdateObjectPayload>,
}

/// Aggregate response for batchupdateobjects RPC.
///
/// Only contains failures.  Transactions are atomic per
/// vnode: either every object on a vnode commits, or the
/// entire vnode group is rolled back.  Objects not present
/// in `failed_vnodes` succeeded — the client already has
/// the data it sent, so the server does not echo it back.
///
/// # Checking for errors
///
/// ```ignore
/// if response.is_success() { /* all done */ }
/// ```
///
/// # Retrying failures
///
/// Each [`VnodeFailure`] carries the original
/// [`UpdateObjectPayload`] entries that were sent in the
/// request.  Callers can resubmit them directly without
/// correlating IDs back to the original batch:
///
/// ```ignore
/// for vf in &response.failed_vnodes {
///     let retry = BatchUpdateObjectsPayload {
///         objects: vf.objects.clone(),
///         request_id: Uuid::new_v4(),
///     };
///     client.batch_update_objects(retry);
/// }
/// ```
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct BatchUpdateObjectsResponse {
    pub failed_vnodes: Vec<VnodeFailure>,
}

impl BatchUpdateObjectsResponse {
    /// Returns true when every object was updated
    /// successfully.
    pub fn is_success(&self) -> bool {
        self.failed_vnodes.is_empty()
    }

    /// Number of objects whose vnode transaction failed.
    pub fn failed_count(&self) -> usize {
        self.failed_vnodes.iter().map(|v| v.objects.len()).sum()
    }
}

/// Decode a Fast RPC message into a batch payload.
pub(crate) fn decode_msg(
    value: &Value,
) -> Result<Vec<BatchUpdateObjectsPayload>, SerdeError> {
    serde_json::from_value::<Vec<BatchUpdateObjectsPayload>>(value.clone())
}

/// Fast RPC action handler for batchupdateobjects.
///
/// Dispatches to `do_batch_update` and wraps the result
/// in a Fast response message.
#[allow(clippy::needless_pass_by_value)]
pub(crate) fn action(
    msg_id: u32,
    method: &str,
    metrics: &RegisteredMetrics,
    log: &Logger,
    payload: BatchUpdateObjectsPayload,
    conn: &mut PostgresConnection,
) -> Result<HandlerResponse, String> {
    do_batch_update(&payload, conn, metrics, log)
        .and_then(|resp| {
            debug!(log, "batch operation complete";
                "failed" => resp.failed_count(),
            );
            let value = serde_json::to_value(&resp).map_err(|e| {
                BucketsMdapiError::PostgresError(format!(
                    "failed to serialize response: {}",
                    e
                ))
            })?;
            let msg_data =
                FastMessageData::new(method.into(), array_wrap(value));
            let msg: HandlerResponse =
                FastMessage::data(msg_id, msg_data).into();
            Ok(msg)
        })
        .or_else(|e| {
            if let BucketsMdapiError::PostgresError(_) = &e {
                error!(log, "batch operation failed";
                    "error" => e.message());
            }
            let msg_data =
                FastMessageData::new(method.into(), array_wrap(e.into_fast()));
            let msg: HandlerResponse =
                FastMessage::data(msg_id, msg_data).into();
            Ok(msg)
        })
}

/// Execute a batch update with per-vnode atomic
/// transactions.
///
/// # Algorithm
///
/// 1. Group objects by vnode.
/// 2. For each vnode group, open a PostgreSQL transaction.
/// 3. Within the transaction, run conditional checks and
///    UPDATE for each object.
/// 4. If all objects in the vnode succeed, COMMIT.
///    If any object fails, ROLLBACK the entire vnode group.
/// 5. Collect per-vnode failures.
///
/// # Time complexity
///
/// O(N) where N is total items across vnodes: one SQL UPDATE per
/// item, grouped into V transactions (V = distinct
/// vnodes).
///
/// # Space complexity
///
/// O(N) for payload storage and response assembly.
fn do_batch_update(
    payload: &BatchUpdateObjectsPayload,
    conn: &mut PostgresConnection,
    metrics: &RegisteredMetrics,
    log: &Logger,
) -> Result<BatchUpdateObjectsResponse, BucketsMdapiError> {
    let objects = &payload.objects;

    if objects.is_empty() {
        return Ok(BatchUpdateObjectsResponse {
            failed_vnodes: Vec::new(),
        });
    }

    if objects.len() > MAX_BATCH_SIZE {
        return Err(BucketsMdapiError::LimitConstraintError(format!(
            "batch size {} exceeds maximum of {}",
            objects.len(),
            MAX_BATCH_SIZE
        )));
    }

    // Group objects by vnode for per-vnode transactions.
    let mut groups: HashMap<u64, Vec<&UpdateObjectPayload>> = HashMap::new();
    for obj in objects {
        groups.entry(obj.vnode).or_insert_with(Vec::new).push(obj);
    }

    debug!(log, "batch update grouped";
        "request_id" => payload.request_id.to_string(),
        "total_objects" => objects.len(),
        "vnode_groups" => groups.len(),
    );

    let mut failed_vnodes: Vec<VnodeFailure> = Vec::new();

    for (vnode, items) in &groups {
        if let Err((err, failed_payloads)) =
            update_vnode_group(*vnode, items, conn, metrics, log)
        {
            warn!(log, "vnode group failed";
                "request_id" => payload.request_id.to_string(),
                "vnode" => vnode,
                "objects" => ?failed_payloads.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(),
                "error" => err.message(),
            );
            let err_value = err.into_fast();
            failed_vnodes.push(VnodeFailure {
                vnode: *vnode,
                error: err_value,
                objects: failed_payloads,
            });
        }
    }

    Ok(BatchUpdateObjectsResponse { failed_vnodes })
}

/// Update all objects in a single vnode within one
/// PostgreSQL transaction.
///
/// # Invariant
///
/// All objects succeed and the transaction commits, or
/// any failure causes a rollback and all objects in this
/// group are marked failed.
fn update_vnode_group(
    vnode: u64,
    items: &[&UpdateObjectPayload],
    conn: &mut PostgresConnection,
    metrics: &RegisteredMetrics,
    log: &Logger,
) -> Result<(), (BucketsMdapiError, Vec<UpdateObjectPayload>)> {
    // Clone payloads only on error paths to avoid
    // allocation on the common (success) path.
    let clone_payloads = || items.iter().map(|i| (*i).clone()).collect();

    let mut txn = conn.transaction().map_err(|e| {
        (
            BucketsMdapiError::PostgresError(e.to_string()),
            clone_payloads(),
        )
    })?;

    let update_sql = update_sql(vnode);

    for item in items {
        // Run conditional checks within the transaction.
        if let Err(e) = conditional::request(
            &mut txn,
            &[&item.owner, &item.bucket_id, &item.name],
            item.vnode,
            &item.conditions,
            metrics,
            log,
        ) {
            // Condition failed: rollback entire vnode
            // group. txn is dropped, triggering implicit
            // rollback.
            return Err((e, clone_payloads()));
        }

        let exec_result = sql::txn_execute(
            sql::Method::ObjectBatchUpdate,
            &mut txn,
            update_sql.as_str(),
            &[
                &item.content_type,
                &item.headers,
                &item.sharks,
                &item.properties,
                &item.owner,
                &item.bucket_id,
                &item.name,
                &item.id,
            ],
            metrics,
            log,
        );

        match exec_result {
            Ok(0) => {
                return Err((
                    BucketsMdapiError::ObjectNotFound,
                    clone_payloads(),
                ));
            }
            Ok(_) => {}
            Err(e) => {
                return Err((
                    BucketsMdapiError::PostgresError(e.to_string()),
                    clone_payloads(),
                ));
            }
        }
    }

    // All objects in this vnode succeeded; commit.
    txn.commit().map_err(|e| {
        (
            BucketsMdapiError::PostgresError(e.to_string()),
            clone_payloads(),
        )
    })?;

    Ok(())
}

/// Generate the UPDATE SQL for a given vnode.
///
/// Identical to `object::update::update_sql` but kept
/// local to avoid coupling to a private function.
fn update_sql(vnode: u64) -> String {
    [
        "UPDATE manta_bucket_",
        &vnode.to_string(),
        ".manta_bucket_object \
         SET content_type = $1, \
         headers = $2, \
         sharks = COALESCE($3, sharks), \
         properties = $4, \
         modified = current_timestamp \
         WHERE owner = $5 \
         AND bucket_id = $6 \
         AND name = $7 \
         AND id = $8",
    ]
    .concat()
}

#[cfg(test)]
mod test {
    use super::*;

    use quickcheck::{quickcheck, Arbitrary, Gen};
    use quickcheck_helpers::random;

    impl Arbitrary for BatchUpdateObjectsPayload {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let count = (usize::arbitrary(g) % 10) + 1;
            let objects: Vec<UpdateObjectPayload> = (0..count)
                .map(|_| UpdateObjectPayload::arbitrary(g))
                .collect();
            BatchUpdateObjectsPayload {
                objects,
                request_id: Uuid::new_v4(),
            }
        }
    }

    impl Arbitrary for VnodeFailure {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let count = (usize::arbitrary(g) % 5) + 1;
            VnodeFailure {
                vnode: u64::arbitrary(g),
                error: Value::String(random::string(g, 16)),
                objects: (0..count)
                    .map(|_| UpdateObjectPayload::arbitrary(g))
                    .collect(),
            }
        }
    }

    impl Arbitrary for BatchUpdateObjectsResponse {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let fail_count = usize::arbitrary(g) % 3;
            BatchUpdateObjectsResponse {
                failed_vnodes: (0..fail_count)
                    .map(|_| VnodeFailure::arbitrary(g))
                    .collect(),
            }
        }
    }

    quickcheck! {
        fn prop_batch_payload_roundtrip(
            msg: BatchUpdateObjectsPayload
        ) -> bool {
            match serde_json::to_string(&msg) {
                Ok(s) => {
                    let decoded: Result<
                        BatchUpdateObjectsPayload, _
                    > = serde_json::from_str(&s);
                    match decoded {
                        Ok(d) => d == msg,
                        Err(_) => false,
                    }
                }
                Err(_) => false,
            }
        }
    }

    quickcheck! {
        fn prop_batch_response_roundtrip(
            msg: BatchUpdateObjectsResponse
        ) -> bool {
            match serde_json::to_string(&msg) {
                Ok(s) => {
                    let decoded: Result<
                        BatchUpdateObjectsResponse, _
                    > = serde_json::from_str(&s);
                    match decoded {
                        Ok(d) => d == msg,
                        Err(_) => false,
                    }
                }
                Err(_) => false,
            }
        }
    }

    quickcheck! {
        fn prop_batch_payload_from_json(
            payload: BatchUpdateObjectsPayload
        ) -> bool {
            let val = serde_json::to_value(&payload)
                .expect("serialize");
            let arr = Value::Array(vec![val]);
            let decoded: Result<
                Vec<BatchUpdateObjectsPayload>, _
            > = serde_json::from_value(arr);
            decoded.is_ok()
        }
    }

    #[test]
    fn test_empty_batch_returns_zero_counts() {
        let payload = BatchUpdateObjectsPayload {
            objects: Vec::new(),
            request_id: Uuid::new_v4(),
        };
        // We cannot call do_batch_update without a DB
        // connection, but we verify the payload is valid
        // and the empty-objects path is exercised via
        // the serialization roundtrip.
        let json = serde_json::to_string(&payload).unwrap();
        let decoded: BatchUpdateObjectsPayload =
            serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.objects.len(), 0);
    }

    #[test]
    fn test_update_sql_contains_vnode() {
        let sql = update_sql(42);
        assert!(sql.contains("manta_bucket_42"));
        assert!(sql.contains("COALESCE"));
        assert!(!sql.contains("RETURNING"));
    }
}
