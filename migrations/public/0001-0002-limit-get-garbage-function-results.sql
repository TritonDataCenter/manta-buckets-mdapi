START TRANSACTION;

SELECT execute($$

CREATE OR REPLACE FUNCTION get_garbage(lmt int DEFAULT 1000)
RETURNS TABLE(schma text, id uuid, owner uuid, bucket_id uuid, name text, created timestamptz, modified timestamptz, content_length bigint, content_md5 bytea, content_type text, headers hstore, sharks text[], properties jsonb) AS $GARBAGE$
DECLARE
        schema RECORD;
        row_count int;
        running_count int := 0;
 BEGIN
      FOR schema IN EXECUTE
          'SELECT schema_name FROM information_schema.schemata WHERE left(schema_name, 13) = ''manta_bucket_'''
      LOOP
           RETURN QUERY EXECUTE
                  format('SELECT ''%I'', id, owner, bucket_id, name, created, modified, content_length, content_md5, content_type, headers, sharks, properties FROM %I.manta_bucket_deleted_object LIMIT %L', schema.schema_name, schema.schema_name, lmt - running_count);

           GET DIAGNOSTICS row_count = ROW_COUNT;
           running_count := running_count + row_count;

           IF running_count >= lmt THEN
              RETURN;
           END IF;
      END LOOP;
END;
$GARBAGE$ LANGUAGE plpgsql;

INSERT INTO migrations (major, minor, note) VALUES (1, 2, 'Correct error in limiting results from get_garbage function');

$$)
WHERE NOT public_migration_exists(1, 2);

COMMIT;
