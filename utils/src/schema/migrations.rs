use std::collections::HashMap;
use std::fs;
use std::io::{Error, ErrorKind};
use std::path::Path;
use std::result::Result;

use itertools::Itertools;
use string_template::Template;

use cueball_postgres_connection::PostgresConnection;

pub(crate) fn run_public_schema_migrations(
    migration_path: &Path,
    conn: &mut PostgresConnection,
) -> Result<(), Error> {
    // List the contents of the public migrations directory, filter out any
    // directory names, sort the files by name, and fold over the migration file
    // names to produce either an `Ok` if all were applied successfully or an
    // `Err` if a problem occurred.
    fs::read_dir(migration_path)?
        .filter(Result::is_ok)
        .map(|dir_entry| dir_entry.unwrap().path())
        .filter(|path_buf| !path_buf.as_path().is_dir())
        .sorted()
        .fold(Ok(()), |acc, migration| {
            if acc.is_ok() {
                let migration_str = fs::read_to_string(migration)?;
                conn.simple_query(&migration_str)
                    .and_then(|_| Ok(()))
                    .map_err(|e| {
                        let err_str =
                            format!("error on public schema migration: {}", e);
                        Error::new(ErrorKind::Other, err_str)
                    })
            } else {
                acc
            }
        })
}

pub(crate) fn run_vnode_schema_migrations(
    vnodes: Vec<&str>,
    migration_path: &Path,
    conn: &mut PostgresConnection,
) -> Result<(), Error> {
    // The vnode migrations directory contains a migration templates that must
    // be run for each vnode.
    //
    // List the contents of the vnode migrations directory, filter out any
    // directory names, sort the files by name, fold over the migration
    // templates names and for each template fold over each vnode. Render the
    // template for each vnode and apply the migration. The result is either an
    // `Ok` if all were applied successfully or an `Err` if a problem occurred.
    fs::read_dir(migration_path)?
        .filter(Result::is_ok)
        .map(|dir_entry| dir_entry.unwrap().path())
        .filter(|path_buf| !path_buf.as_path().is_dir())
        .sorted()
        .fold(Ok(()), |acc, migration| {
            if acc.is_ok() {
                let migration_template = fs::read_to_string(migration)?;
                let template = Template::new(&migration_template);
                vnodes.iter().fold(Ok(()), |vnode_acc, vnode| {
                    if vnode_acc.is_ok() {
                        let mut args = HashMap::new();
                        args.insert("vnode", *vnode);
                        let migration_str = template.render(&args);
                        conn.simple_query(&migration_str)
                            .and_then(|_| Ok(()))
                            .map_err(|e| {
                                let err_str = format!(
                                    "error on public schema migration: {}",
                                    e
                                );
                                Error::new(ErrorKind::Other, err_str)
                            })
                    } else {
                        vnode_acc
                    }
                })
            } else {
                acc
            }
        })
}
