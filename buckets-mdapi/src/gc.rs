// Copyright 2020 Joyent, Inc.

use uuid::Uuid;

use crate::types::RowSlice;

pub mod delete;
pub mod get;

pub(crate) fn refresh_garbage_view_sql() -> &'static str {
    "REFRESH MATERIALIZED VIEW GARBAGE_BATCH"
}

pub(crate) fn get_garbage_batch_id_sql() -> &'static str {
    "SELECT batch_id FROM garbage_batch_id WHERE id = 1"
}

pub(crate) fn update_garbage_batch_id_sql() -> &'static str {
    "UPDATE garbage_batch_id SET batch_id = $1 WHERE id = 1"
}

pub(crate) fn handle_batch_id_result(rows: &RowSlice) -> Result<Uuid, String> {
    if rows.len() == 1 {
        // let mut batch_id: Option<Uuid> = None;
        let batch_id = rows[0].get("batch_id");
        Ok(batch_id)
    } else if rows.is_empty() {
        Err("garbage batch id not found".into())
    } else {
        Err(
            "database invariant failure: found more than one garbage batch id"
                .into(),
        )
    }
}
