// Copyright 2019 Joyent, Inc.

pub mod delete;
pub mod get;

pub fn refresh_garbage_view_sql() -> &'static str {
    "REFRESH MATERIALIZED VIEW GARBAGE_BATCH"
}
