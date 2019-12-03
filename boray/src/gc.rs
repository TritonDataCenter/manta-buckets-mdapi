// Copyright 2019 Joyent, Inc.

pub mod delete;
pub mod get;

use slog::Logger;
use uuid::Uuid;

use cueball_postgres_connection::PostgresConnection;

use crate::sql;

pub fn create_garbage_infra(conn: &mut PostgresConnection, log: &Logger) -> Result<(), String> {
    let mut txn = (*conn).transaction().map_err(|e| e.to_string())?;

    // First create the gc helper function
    sql::txn_execute(
        sql::Method::GarbageGet,
        &mut txn,
        create_get_garbage_function_sql(),
        &[],
        log,
    )
    .and_then(|_| {
        // Attempt to create the materialized view
        sql::txn_execute(
            sql::Method::GarbageGet,
            &mut txn,
            create_garbage_view_sql(),
            &[],
            log,
        )
    })
    .and_then(|_| {
        // Attempt to create the garbage batch id table
        sql::txn_execute(
            sql::Method::GarbageGet,
            &mut txn,
            create_garbage_batch_id_table_sql(),
            &[],
            log,
        )
    })
    .and_then(|_| {
        // Initialize the garbage batch id
        let batch_id = Uuid::new_v4();
        sql::txn_execute(
            sql::Method::GarbageGet,
            &mut txn,
            initialize_garbage_batch_id_sql(),
            &[&batch_id],
            log,
        )
    })
    .and_then(|_| {
        txn.commit()?;
        Ok(())
    })
    .map_err(|e| e.to_string())
}

pub fn create_garbage_view(conn: &mut PostgresConnection, log: &Logger) -> Result<(), String> {
    let mut txn = (*conn).transaction().map_err(|e| e.to_string())?;

    sql::txn_execute(
        sql::Method::BucketDeleteMove,
        &mut txn,
        create_garbage_view_sql(),
        &[],
        log,
    )
    .and_then(|_| {
        txn.commit()?;
        Ok(())
    })
    .map_err(|e| e.to_string())
}

fn create_garbage_view_sql() -> &'static str {
    "CREATE MATERIALIZED VIEW GARBAGE_BATCH AS ( \
     SELECT * FROM get_garbage(1000) \
     ) \
     WITH DATA"
}

fn create_garbage_batch_id_table_sql() -> &'static str {
    "CREATE TABLE garbage_batch_id (id integer, batch_id UUID)"
}

fn initialize_garbage_batch_id_sql() -> &'static str {
    "INSERT INTO garbage_batch_id (id, batch_id) VALUES (1, $1)"
}

fn create_get_garbage_function_sql() -> &'static str {
    "CREATE OR REPLACE FUNCTION get_garbage(lmt int DEFAULT 1000) \
     RETURNS TABLE(schma text, id uuid, owner uuid, bucket_id uuid, name text, created timestamptz, modified timestamptz, content_length bigint, content_md5 bytea, content_type text, headers hstore, sharks text[], properties jsonb) AS $$ \
     DECLARE \
         schema RECORD; \
     BEGIN \
         FOR schema IN EXECUTE \
             'SELECT schema_name FROM information_schema.schemata \
             WHERE left(schema_name, 13) = ''manta_bucket_''' \
         LOOP \
             RETURN QUERY EXECUTE \
                 format('SELECT ''%I'', id, owner, bucket_id, name, created, modified, content_length, content_md5, content_type, headers, sharks, properties FROM %I.manta_bucket_deleted_object LIMIT %L', schema.schema_name, schema.schema_name, lmt); \
         END LOOP; \
     END; \
     $$ LANGUAGE plpgsql"
}
