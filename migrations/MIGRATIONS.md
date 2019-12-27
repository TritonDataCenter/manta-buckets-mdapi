# Database migrations for buckets-mdapi

## Overview

Migrations are a necessary complication of storing Manta object metadata as
structured data in a relational database. Even with the best planning the needs
of an application may change or expand over time necessitating a change to the
layout of the database. The goal of a database migrations scheme is to
facilitate the process of evolving the database to meet the changing needs of
the application.

The scheme chosen for `buckets-mdapi` uses two distinct sets of database
migration scripts (migrations) as well as a `Major.Minor` versioning scheme. The
reason that there are two sets of migrations is that each `buckets-mdapi`
instance manages a set of database schemas that represent the virtual nodes
(vnodes) that belong to the metadata shard. There is a set of *vnode* migrations
that apply to all vnode schemas for the shard. There are also a set of
migrations that apply to the `public` schema. This is the default schema in
PostgreSQL and for the purposes of `buckets-mdapi` it serves as the home for
aspects of the database that are vnode-agnostic or apply across all vnodes
(*e.g.* A helper function for operators that provides a means to query all the
object metadata on a shard). The `public` set of migrations are written as SQL
files that could be used as input to the `psql -f` command. The `vnode` set of
schemas are represented as templates that can be populated with the schema name
for each vnode on the shard.


## Versioning

As mentioned in the previous section `buckets-mdapi` uses a `Major.Minor`
versioning scheme for migrations.  This allows for a distinction between
breaking and non-breaking schema migrations. The idea is similar to semantic
versioning for software packages and allows us to only go through the extra
deployment steps for breaking schema changes when absolutely necessary. A major
version change is needed for any schema change that breaks the existing queries
used by `buckets-mdapi`. Purely additive changes (*e.g.* Adding a new table for
a new feature) or performance-related changes (*e.g.* Adding a new index) are
not breaking changes and represent only minor version changes. Minor version
changes **must** be able to be safely applied without impacting any
`buckets-mdapi` code. The onus is on the developers to ensure this is the case.

A major version change requires more coordination and work to handle
correctly. Major version changes require feature gating code such that the
migration can be applied without breaking the currently deployed buckets-mdapi
instances. This means the first step in deploying a major schema version change
must be to update all `buckets-mdapi` instances with an image featuring the
feature-gated code.

Once this deployment step is complete an image with the breaking migration may
be applied. This moves the database to the latest migration level. One of the
advantages of this scheme is that after the major version migration is deployed
the feature-gated code may be removed and a `buckets-mdapi` image that only uses
the latest version of the schema may be deployed. This relieves us of the burden
of maintaining the feature-gated code long term which could become a very messy
and complex situation. The reason this works is that the schema manager will
start any new shards at the most recently created migration level from the start
so the feature gating is not necessary for new shards. At the same time the
schema manager always wants to move existing shard databases to the most recent
migration level as well so even if the deployment process was done in the wrong
order the problem can always be resolved by moving to the latest image that
would include the most recent migration.

## Adding a new migration

### Major version change

1. Make and commit changes needed to feature gate the buckets-mdapi code.
1. Build and deploy image with the changes
1. Make commit with new migration with a properly incremented major version
   number (both in the file name and in the migration itself). Reset the minor
   version to zero for each new major version.
1. Deploy new image with the migration
1. Commit change removing featured gated code
1. Deploy the new image (though there is not necessarily urgency to this step)

### Minor version change

1. Make commit with new migration with a properly incremented minor version
   number (both in the file name and in the migration itself).
1. Deploy new image with the migration

## Downgrades

Each migration should be created with a *downgrade* script that can be used in
an emergency to reverse the changes done by the migration. *e.g.* If a migration
creates a new table then the downgrade script should drop that table.

Downgrade scripts for the *public* schema migrations are straightforward to
create and apply, but just like the vnode schema migrations must be represented
as templates so too must the vnode schema downgrade scripts. Due to this fact a
helper tool is needed to be able to apply a downgrade template for the vnode
schemas. This tool is not yet available and this section will be updated once it
is.

Applying a *public* schema downgrade can be done by using `psql` in the
following manner:
```
psql -U buckets_mdapi buckets_metadata -f ./migrations/public/downgrades/0001-0000-garbage-collection-setup-downgrade.sql
```
