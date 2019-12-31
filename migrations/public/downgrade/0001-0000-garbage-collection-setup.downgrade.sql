START TRANSACTION;

DROP TABLE garbage_batch_id;

DROP MATERIALIZED VIEW GARBAGE_BATCH;

DROP FUNCTION get_garbage(int);

DELETE FROM public.migrations WHERE major = 1 and minor = 0;

COMMIT;
