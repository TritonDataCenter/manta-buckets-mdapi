START TRANSACTION;

SELECT execute($$

ALTER TABLE garbage_batch_id ADD PRIMARY KEY (id);

INSERT INTO migrations (major, minor, note) VALUES (1, 4, 'Add primary key to garbage_batch_id table');

$$)
WHERE NOT public_migration_exists(1, 4);

COMMIT;
