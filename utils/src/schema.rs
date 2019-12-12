use std::collections::HashMap;
use std::fs;
use std::io::{Error, ErrorKind};
use std::path::Path;

use slog::{error, info, o, Logger};
use string_template::Template;

use cueball::connection_pool::types::ConnectionPoolOptions;
use cueball::connection_pool::ConnectionPool;
use cueball::resolver::Resolver;
use cueball_postgres_connection::{
    PostgresConnection, PostgresConnectionConfig,
};

use crate::config::{tls, ConfigDatabase};

pub mod migrations;

const SCHEMA_TEMPLATE: &str = "schema.in";
const ADMIN_TEMPLATE: &str = "admin.in";
const DB_TEMPLATE: &str = "db.in";

// create users, role, database and schemas
pub fn create_bucket_schemas<R>(
    admin_conn: &mut PostgresConnection,
    database_config: &ConfigDatabase,
    resolver: R,
    template_dir: &str,
    migrations_dir: &Path,
    vnodes: Vec<&str>,
    log: &Logger,
) -> Result<(), Error>
where
    R: Resolver,
{
    let schema_template_path = [template_dir, "/", SCHEMA_TEMPLATE].concat();
    let admin_template_path = [template_dir, "/", ADMIN_TEMPLATE].concat();
    let db_template_path = [template_dir, "/", DB_TEMPLATE].concat();
    let schema_template = fs::read_to_string(&schema_template_path)?;

    let template = Template::new(&schema_template);

    info!(log, "Creating boray user and role if they don't exist");

    create_user_role(admin_conn, &admin_template_path)
        .and_then(|_| {
            info!(log, "Creating boray database if it doesn't exist");
            create_database(admin_conn, &db_template_path)
        })
        .and_then(|_| {
            // Attempt to construct the TLS configuration for postgres
            info!(log, "Constructing postgres TLS configuration");

            tls::tls_config(
                database_config.tls_mode.clone(),
                database_config.certificate.clone(),
            )
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))
        })
        .and_then(|tls_config| {
            // Create the connection pool to the boray database using the
            // boray role
            info!(log, "Creating a connection pool to the boray database");

            let pg_config = PostgresConnectionConfig {
                user: Some(database_config.user.clone()),
                password: None,
                host: None,
                port: None,
                database: Some(database_config.database.clone()),
                application_name: Some("schema-manager".into()),
                tls_config,
            };

            let connection_creator =
                PostgresConnection::connection_creator(pg_config);

            let pool_opts = ConnectionPoolOptions {
                max_connections: Some(1),
                claim_timeout: Some(10000),
                log: Some(log.new(o!(
                    "component" => "CueballConnectionPool"
                ))),
                rebalancer_action_delay: None,
                decoherence_interval: None,
            };

            let pool =
                ConnectionPool::new(pool_opts, resolver, connection_creator);

            pool.claim()
                .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))
        })
        .and_then(|mut conn| {
            info!(log, "Creating boray schemas");
            let mut result: Result<(), Error> = Ok(());
            for vnode in &vnodes {
                info!(log, "processing vnode: {}", vnode);
                let mut args = HashMap::new();
                args.insert("vnode", *vnode);
                let schema_str = template.render(&args);
                result = conn
                    .simple_query(&schema_str)
                    .map_err(|e| {
                        let err_str = format!(
                            "error on schema creation: {}, vnode: {}",
                            e, vnode
                        );
                        Error::new(ErrorKind::Other, err_str)
                    })
                    .map(|_| ());
            }
            result.and_then(|_| Ok(conn))
        })
        .and_then(|mut conn| {
            // Run the public schema migrations
            info!(log, "Running public schema migrations");
            let public_migrations_dir = migrations_dir.join("public");
            migrations::run_public_schema_migrations(
                &public_migrations_dir,
                &mut conn,
            )
            .unwrap();
            Ok(conn)
        })
        .and_then(|mut conn| {
            info!(log, "Running vnode schema migrations");
            let vnode_migrations_dir = migrations_dir.join("vnode");
            migrations::run_vnode_schema_migrations(
                vnodes,
                &vnode_migrations_dir,
                &mut conn,
            )
        })
        .or_else(|e| {
            error!(log, "{}", e);
            Err(e)
        })
}

// create the db in its own transaction
fn create_database(
    conn: &mut PostgresConnection,
    db_template: &str,
) -> Result<(), Error> {
    let db_str = fs::read_to_string(db_template)?;
    db_create_object(conn, &db_str).map_err(|e| {
        let err_str = format!("error on database creation: {}", e);
        std::io::Error::new(ErrorKind::Other, err_str)
    })
}

// create the role in its own transaction
fn create_user_role(
    conn: &mut PostgresConnection,
    admin_template: &str,
) -> Result<(), Error> {
    let admin_str = fs::read_to_string(admin_template)?;
    db_create_object(conn, &admin_str).map_err(|e| {
        let err_str = format!("error on role creation: {}", e);
        std::io::Error::new(ErrorKind::Other, err_str)
    })
}

// Generic DB create function
fn db_create_object(
    conn: &mut PostgresConnection,
    sql: &str,
) -> Result<(), String> {
    conn.simple_query(&sql).and_then(|_| Ok(())).or_else(|e| {
        let err_str = e.to_string();
        if err_str.contains("already exists") {
            Ok(())
        } else {
            Err(err_str)
        }
    })
}
