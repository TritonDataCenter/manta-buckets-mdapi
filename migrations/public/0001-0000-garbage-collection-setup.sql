START TRANSACTION;

SELECT execute($$

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE OR REPLACE FUNCTION get_garbage(lmt int DEFAULT 1000)
RETURNS TABLE(schma text, id uuid, owner uuid, bucket_id uuid, name text, created timestamptz, modified timestamptz, content_length bigint, content_md5 bytea, content_type text, headers hstore, sharks text[], properties jsonb) AS $GARBAGE$
DECLARE
        schema RECORD;
BEGIN
      FOR schema IN EXECUTE
          'SELECT schema_name FROM information_schema.schemata WHERE left(schema_name, 13) = ''manta_bucket_'''
      LOOP
           RETURN QUERY EXECUTE
                  format('SELECT ''%I'', id, owner, bucket_id, name, created, modified, content_length, content_md5, content_type, headers, sharks, properties FROM %I.manta_bucket_deleted_object LIMIT %L', schema.schema_name, schema.schema_name, lmt);
      END LOOP;
END;
$GARBAGE$ LANGUAGE plpgsql;

CREATE MATERIALIZED VIEW GARBAGE_BATCH AS (
       SELECT * FROM get_garbage(1000)
)
WITH DATA;

CREATE TABLE garbage_batch_id (id integer, batch_id UUID);

INSERT INTO garbage_batch_id (id, batch_id) VALUES (1, gen_random_uuid());

INSERT INTO migrations (major, minor, note) VALUES (1, 0, 'Add garbage collection infrastructure');

$$)
WHERE NOT public_migration_exists(1, 0);

COMMIT;
