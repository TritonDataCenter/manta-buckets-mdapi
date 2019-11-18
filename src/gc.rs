// Copyright 2019 Joyent, Inc.

pub mod delete;
pub mod get;

use cueball_postgres_connection::PostgresConnection;

use crate::sql;

pub fn create_list_all_deleted_objects_fn(conn: &mut PostgresConnection) -> Result<(), String> {
    let mut txn = (*conn).transaction().map_err(|e| e.to_string())?;

    sql::txn_execute(
        sql::Method::BucketDeleteMove,
        &mut txn,
        list_all_deleted_objects_sql(),
        &[],
    )
    .and_then(|_| {
        txn.commit()?;
        Ok(())
    })
    .map_err(|e| e.to_string())
}

fn list_all_deleted_objects_sql() -> &'static str {
    "CREATE OR REPLACE FUNCTION list_all_deleted_objects(lmt int DEFAULT 100) \
     RETURNS TABLE(id uuid, name text, owner uuid, bucket_id uuid, created \
     timestamptz, modified timestamptz, content_length \
     bigint, content_md5 bytea, content_type text, headers hstore, sharks text[], \
     properties jsonb) AS $$ \
     DECLARE \
       schema RECORD; \
     BEGIN \
       FOR schema IN EXECUTE \
         'SELECT schema_name \
          FROM information_schema.schemata \
          WHERE left(schema_name, 13) = ''manta_bucket_''' \
       LOOP \
         RETURN QUERY EXECUTE \
           format('SELECT id,name,owner,bucket_id,created,modified,content_length,content_md5::bytea,content_type,headers,sharks,properties FROM %I.manta_bucket_deleted_object LIMIT %L', schema.schema_name, lmt); \
       END LOOP; \
     END; \
     $$ LANGUAGE plpgsql;"
}
