START TRANSACTION;

SELECT execute($$

CREATE OR REPLACE FUNCTION list_all_objects(lmt int DEFAULT 100)
RETURNS TABLE(schma text, id uuid, name text, owner uuid, bucket_id uuid, created
timestamptz, modified timestamptz, content_length
bigint, content_md5 bytea, content_type text, headers hstore, sharks text[],
properties jsonb) AS $$
DECLARE
  schema RECORD;
BEGIN
  FOR schema IN EXECUTE
      'SELECT schema_name FROM information_schema.schemata WHERE left(schema_name, 13) = ''manta_bucket_'''
  LOOP
    RETURN QUERY EXECUTE
      format('SELECT ''%I'', id, name, owner, bucket_id, created, modified, content_length, content_md5::bytea, content_type, headers, sharks, properties FROM %I.manta_bucket_object LIMIT %L', schema.schema_name, schema.schema_name, lmt);
  END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION list_all_buckets(lmt int DEFAULT 100)
RETURNS TABLE(schma text, id uuid, name text, owner uuid, created timestamptz) AS $$
DECLARE
  schema RECORD;
BEGIN
  FOR schema IN EXECUTE
      'SELECT schema_name FROM information_schema.schemata WHERE left(schema_name, 13) = ''manta_bucket_'''
  LOOP
    RETURN QUERY EXECUTE
      format('SELECT ''%I'', id, name, owner, created FROM %I.manta_bucket LIMIT %L', schema.schema_name, schema.schema_name, lmt);
  END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bucket_listing(o_id uuid, b_id uuid, lmt int DEFAULT 100)
RETURNS TABLE(schma text, id uuid, name text, owner uuid, bucket_id uuid, created
timestamptz, modified timestamptz, content_length
bigint, content_md5 bytea, content_type text, headers hstore, sharks text[],
properties jsonb) AS $$
DECLARE
  schema RECORD;
BEGIN
  FOR schema IN EXECUTE
      'SELECT schema_name FROM information_schema.schemata WHERE left(schema_name, 13) = ''manta_bucket_'''
  LOOP
    RETURN QUERY EXECUTE
      format('SELECT ''%I'', id, name, owner, bucket_id, created, modified, content_length, content_md5::bytea, content_type, headers, sharks, properties FROM %I.manta_bucket_object WHERE owner = %L and
bucket_id = %L LIMIT %L', schema.schema_name, schema.schema_name, o_id, b_id, lmt);
  END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION list_all_deleted_objects(lmt int DEFAULT 1000)
RETURNS TABLE(schma text, id uuid, owner uuid, bucket_id uuid, name text, created timestamptz, modified timestamptz, content_length bigint, content_md5 bytea, content_type text, headers hstore, sharks text[], properties jsonb) AS $$
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
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION list_all_deleted_buckets(lmt int DEFAULT 100)
RETURNS TABLE(schma text, id uuid, name text, owner uuid, created timestamptz) AS $$
DECLARE
  schema RECORD;
BEGIN
  FOR schema IN EXECUTE
      'SELECT schema_name FROM information_schema.schemata WHERE left(schema_name, 13) = ''manta_bucket_'''
  LOOP
    RETURN QUERY EXECUTE
      format('SELECT ''%I'', id, name, owner, created FROM %I.manta_bucket_deleted_bucket LIMIT %L', schema.schema_name, schema.schema_name, lmt);
  END LOOP;
END;
$$ LANGUAGE plpgsql;

INSERT INTO migrations (major, minor, note) VALUES (1, 1, 'Create helper functions for operators');

$$)
WHERE NOT public_migration_exists(1, 1);

COMMIT;
