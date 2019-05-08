BEGIN;

CREATE SCHEMA manta_bucket_0;
CREATE SCHEMA manta_bucket_1;


CREATE TABLE manta_bucket_0.manta_bucket (
    id uuid NOT NULL,
    name text NOT NULL,
    owner uuid NOT NULL,
    created timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,

    PRIMARY KEY (owner, name)
);

CREATE TABLE manta_bucket_0.manta_bucket_deleted_bucket (
    id uuid NOT NULL,
    name text NOT NULL,
    owner uuid NOT NULL,
    created timestamp with time zone NOT NULL,
    deleted_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE TABLE manta_bucket_0.manta_bucket_deleted_object (
    id uuid NOT NULL,
    name text NOT NULL,
    owner uuid NOT NULL,
    bucket_id uuid NOT NULL,
    created timestamp with time zone NOT NULL,
    modified timestamp with time zone NOT NULL,
    creator uuid,
    content_length bigint,
    content_md5 bytea,
    content_type text,
    headers hstore,
    sharks text[],
    properties jsonb,
    deleted_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE TABLE manta_bucket_0.manta_bucket_object (
    id uuid NOT NULL,
    name text NOT NULL,
    owner uuid NOT NULL,
    bucket_id uuid NOT NULL,
    created timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    modified timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    creator uuid,
    content_length bigint,
    content_md5 bytea,
    content_type text,
    headers hstore,
    sharks text[],
    properties jsonb,

    PRIMARY KEY (owner, bucket_id, name)
);

CREATE INDEX ON manta_bucket_0.manta_bucket_deleted_object USING btree (deleted_at);

CREATE INDEX ON manta_bucket_0.manta_bucket_deleted_object USING btree (id);

CREATE TABLE manta_bucket_1.manta_bucket (
    id uuid NOT NULL,
    name text NOT NULL,
    owner uuid NOT NULL,
    created timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,

    PRIMARY KEY (owner, name)
);

CREATE TABLE manta_bucket_1.manta_bucket_deleted_bucket (
    id uuid NOT NULL,
    name text NOT NULL,
    owner uuid NOT NULL,
    created timestamp with time zone NOT NULL,
    deleted_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE TABLE manta_bucket_1.manta_bucket_deleted_object (
    id uuid NOT NULL,
    name text NOT NULL,
    owner uuid NOT NULL,
    bucket_id uuid NOT NULL,
    created timestamp with time zone NOT NULL,
    modified timestamp with time zone NOT NULL,
    creator uuid,
    content_length bigint,
    content_md5 bytea,
    content_type text,
    headers hstore,
    sharks text[],
    properties jsonb,
    deleted_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE TABLE manta_bucket_1.manta_bucket_object (
    id uuid NOT NULL,
    name text NOT NULL,
    owner uuid NOT NULL,
    bucket_id uuid NOT NULL,
    created timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    modified timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    creator uuid,
    content_length bigint,
    content_md5 bytea,
    content_type text,
    headers hstore,
    sharks text[],
    properties jsonb,

    PRIMARY KEY (owner, bucket_id, name)
);

CREATE INDEX ON manta_bucket_1.manta_bucket_deleted_object USING btree (deleted_at);

CREATE INDEX ON manta_bucket_1.manta_bucket_deleted_object USING btree (id);

COMMIT;
