START TRANSACTION;

CREATE TABLE IF NOT EXISTS public.migrations (
    major               integer,
    minor               integer,
    applied             timestamptz NOT NULL DEFAULT current_timestamp,
    -- an optional note about why this migration is needed
    note                text,

    PRIMARY KEY (major, minor)
);

-- used to conditionally execude a DDL:
-- inspired by: http://www.depesz.com/2008/06/18/conditional-ddl/
CREATE OR REPLACE FUNCTION execute(TEXT) RETURNS VOID AS $$
BEGIN EXECUTE $1; END;
$$ LANGUAGE plpgsql STRICT;

-- Used to see if a particular migration id exists in the public schema, inspired by:
-- http://www.depesz.com/2008/06/18/conditional-ddl/
CREATE OR REPLACE FUNCTION public_migration_exists(INTEGER, INTEGER) RETURNS bool as $$
SELECT exists(SELECT 1 FROM public.migrations WHERE major = $1 AND minor = $2);
$$ language sql STRICT;

-- Used to see if a particular migration id exists for a vnode schema, inspired
-- by: http://www.depesz.com/2008/06/18/conditional-ddl/
CREATE OR REPLACE FUNCTION vnode_migration_exists(vnode TEXT, major INTEGER, minor INTEGER) RETURNS bool as $$
DECLARE
        result bool;
BEGIN
        EXECUTE format('SELECT exists(SELECT 1 FROM %I.migrations WHERE major = %L AND minor = %L)', vnode, major, minor)
        INTO STRICT result;
        RETURN result;
END
$$ LANGUAGE plpgsql;

COMMIT;
