// Copyright 2020 Joyent, Inc.
// Copyright 2023 MNX Cloud, Inc.
// Copyright 2026 Edgecast Cloud LLC.

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr};
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Once};
use std::thread;
use std::time::Instant;

use rand::Rng;
use serde_json::json;
use slog::{error, info, o, Drain, Level, LevelFilter, Logger};
use url::Url;
use uuid::Uuid;

use cueball::connection_pool::types::ConnectionPoolOptions;
use cueball::connection_pool::ConnectionPool;
use cueball_postgres_connection::{
    PostgresConnection, PostgresConnectionConfig, TlsConfig,
};
use cueball_static_resolver::StaticIpResolver;
use fast_rpc::protocol::{FastMessage, FastMessageData};

use buckets_mdapi::bucket;
use buckets_mdapi::conditional;
use buckets_mdapi::discovery;
use buckets_mdapi::error::{BucketsMdapiError, BucketsMdapiWrappedError};
use buckets_mdapi::gc;
use buckets_mdapi::metrics;
use buckets_mdapi::metrics::RegisteredMetrics;
use buckets_mdapi::object;
use buckets_mdapi::util;
use utils::{config, schema};

// Prometheus metrics are registered into a global registry.
// When tests run in parallel, each test must share the same
// RegisteredMetrics to avoid duplicate-registration panics.
// Uses static mut + Once because Mutex::new is not const fn
// in Rust 1.40 (the target toolchain on SmartOS).
static METRICS_INIT: Once = Once::new();
static mut METRICS_STORE: Option<RegisteredMetrics> = None;

fn shared_metrics() -> RegisteredMetrics {
    unsafe {
        METRICS_INIT.call_once(|| {
            let cfg = config::ConfigMetrics::default();
            METRICS_STORE = Some(metrics::register_metrics(&cfg));
        });
        METRICS_STORE.clone().expect("metrics not initialized")
    }
}

// Create an ephemeral PostgreSQL database, connection pool,
// and vnode schemas for integration testing.
//
// Requires pg_tmp (ephemeralpg) to be installed.
//
// Implemented as a macro because ConnectionPool contains
// an unnameable closure type from connection_creator().
// The macro expands inline so the pool type is inferred.
macro_rules! setup_test_env {
    ($pool:ident, $metrics:ident, $log:ident) => {
        let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
        let $log = Logger::root(
            Mutex::new(LevelFilter::new(
                slog_term::FullFormat::new(plain).build(),
                Level::Error,
            ))
            .fuse(),
            o!(),
        );

        let $metrics = shared_metrics();

        let pg_tmp_check_output = Command::new("which")
            .arg("pg_tmp")
            .output()
            .expect("failed to run 'which pg_tmp' — ensure 'which' is available on PATH");

        if !pg_tmp_check_output.status.success() {
            error!($log, "pg_tmp is required to run this test");
        }
        assert!(pg_tmp_check_output.status.success());

        let create_db_output =
            Command::new("../tools/postgres/create-ephemeral-db.sh")
                .output()
                .expect("failed to run '../tools/postgres/create-ephemeral-db.sh' — check the script exists and is executable");

        assert!(create_db_output.status.success());

        let pg_connect_str = String::from_utf8_lossy(&create_db_output.stdout);

        info!($log, "pg url: {}", pg_connect_str);

        let pg_url = Url::parse(&pg_connect_str)
            .expect("failed to parse postgres connection string");

        let pg_port = pg_url.port().expect("failed to parse postgres port");
        let pg_db = "test";
        let user = "postgres";
        let application_name = "buckets_mdapi_test";

        let pg_config = PostgresConnectionConfig {
            user: Some(user.into()),
            password: None,
            host: None,
            port: Some(pg_port),
            database: Some(pg_db.into()),
            application_name: Some(application_name.into()),
            tls_config: TlsConfig::disable(),
        };

        let connection_creator =
            PostgresConnection::connection_creator(pg_config);
        let pool_opts = ConnectionPoolOptions {
            max_connections: Some(5),
            claim_timeout: None,
            log: Some($log.clone()),
            rebalancer_action_delay: None,
            decoherence_interval: None,
            connection_check_interval: None,
        };

        let primary_backend =
            (IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), pg_port);
        let resolver = StaticIpResolver::new(vec![primary_backend]);

        let $pool =
            ConnectionPool::new(pool_opts, resolver, connection_creator);

        let template_dir = "../schema_templates";
        let migrations_dir = Path::new("../migrations");

        let mut conn = $pool
            .claim()
            .expect("failed to acquire connection for schema setup");

        let config = config::ConfigDatabase {
            port: pg_port,
            database: pg_db.to_owned(),
            ..Default::default()
        };

        let vnode_resolver = StaticIpResolver::new(vec![primary_backend]);

        schema::create_bucket_schemas(
            &mut conn,
            &config,
            vnode_resolver,
            template_dir,
            migrations_dir,
            ["0", "1"].to_vec(),
            &$log,
        )
        .expect("failed to create vnode schemas");

        drop(conn);
    };
}

// Helper macros for common setup operations within tests
// that already have pool, metrics, and log in scope.

macro_rules! create_test_bucket {
    ($msg_id:expr, $owner_id:expr, $bucket:expr,
     $vnode:expr, $request_id:expr,
     $pool:expr, $metrics:expr, $log:expr) => {{
        let payload = bucket::create::CreateBucketPayload {
            owner: $owner_id,
            name: $bucket.into(),
            vnode: $vnode,
            request_id: $request_id,
        };
        let json = serde_json::to_value(vec![payload]).unwrap();
        let msg_data = FastMessageData::new("createbucket".into(), json);
        let msg = FastMessage::data($msg_id, msg_data);
        let result = util::handle_msg(&msg, &$pool, &$metrics, &$log);
        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.len(), 1);
        let resp: Result<bucket::BucketResponse, _> =
            serde_json::from_value(response[0].data.d[0].clone());
        assert!(resp.is_ok());
        assert_eq!(resp.unwrap().name, $bucket);
    }};
}

macro_rules! create_test_object {
    ($msg_id:expr, $owner_id:expr, $bucket_id:expr,
     $object_name:expr, $object_id:expr, $vnode:expr,
     $request_id:expr,
     $pool:expr, $metrics:expr, $log:expr) => {{
        let shark1 = object::StorageNodeIdentifier {
            datacenter: "us-east-1".into(),
            manta_storage_id:
                "1.stor.us-east.joyent.com".into(),
        };
        let shark2 = object::StorageNodeIdentifier {
            datacenter: "us-east-2".into(),
            manta_storage_id:
                "3.stor.us-east.joyent.com".into(),
        };
        let conditions = serde_json::from_value::<
            conditional::Conditions,
        >(json!({
            "if-none-match": [ "*" ]
        }))
        .unwrap();
        let payload = object::create::CreateObjectPayload {
            owner: $owner_id,
            bucket_id: $bucket_id,
            name: $object_name.into(),
            id: $object_id,
            vnode: $vnode,
            content_length: 5,
            content_md5: "xzY5jJbR9rcrMRhlcmi/8g==".into(),
            content_type: "text/plain".into(),
            headers: HashMap::new(),
            sharks: vec![shark1, shark2],
            properties: None,
            request_id: $request_id,
            conditions,
        };
        let json =
            serde_json::to_value(vec![payload]).unwrap();
        let msg_data = FastMessageData::new(
            "createobject".into(),
            json,
        );
        let msg = FastMessage::data($msg_id, msg_data);
        let result = util::handle_msg(
            &msg, &$pool, &$metrics, &$log,
        );
        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.len(), 1);
        let resp: Result<object::ObjectResponse, _> =
            serde_json::from_value(
                response[0].data.d[0].clone(),
            );
        assert!(resp.is_ok());
        assert_eq!(&resp.unwrap().name, $object_name);
    }};
}

macro_rules! delete_test_object {
    ($msg_id:expr, $owner_id:expr, $bucket_id:expr,
     $object_name:expr, $vnode:expr, $request_id:expr,
     $pool:expr, $metrics:expr, $log:expr) => {{
        let payload = object::GetObjectPayload {
            owner: $owner_id,
            bucket_id: $bucket_id,
            name: $object_name.into(),
            vnode: $vnode,
            request_id: $request_id,
            conditions: Default::default(),
        };
        let json = serde_json::to_value(vec![payload]).unwrap();
        let msg_data = FastMessageData::new("deleteobject".into(), json);
        let msg = FastMessage::data($msg_id, msg_data);
        let result = util::handle_msg(&msg, &$pool, &$metrics, &$log);
        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.len(), 1);
        let resp: Result<Vec<object::DeleteObjectResponse>, _> =
            serde_json::from_value(response[0].data.d[0].clone());
        assert!(resp.is_ok());
        let items = resp.unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(&items[0].owner, &$owner_id);
        assert_eq!(&items[0].bucket_id, &$bucket_id);
        assert_eq!(&items[0].name, $object_name);
    }};
}

// This test suite requires PostgreSQL and pg_tmp
// (http://eradman.com/ephemeralpg/) to be installed on
// the test system.

////////////////////////////////////////////////////////////
// Bucket CRUD tests
////////////////////////////////////////////////////////////

#[test]
fn verify_bucket_handlers() {
    setup_test_env!(pool, metrics, log);
    let msg_id: u32 = 0x1;
    let owner_id = Uuid::new_v4();
    let bucket: String = "testbucket".into();
    let request_id = Uuid::new_v4();

    // Get nonexistent bucket
    let get_bucket_payload = bucket::GetBucketPayload {
        owner: owner_id,
        name: bucket.clone(),
        vnode: 0,
        request_id,
    };

    let get_bucket_json =
        serde_json::to_value(vec![get_bucket_payload]).unwrap();
    let get_bucket_fast_msg_data =
        FastMessageData::new("getbucket".into(), get_bucket_json);
    let get_bucket_fast_msg =
        FastMessage::data(msg_id, get_bucket_fast_msg_data);
    let mut get_bucket_result =
        util::handle_msg(&get_bucket_fast_msg, &pool, &metrics, &log);

    assert!(get_bucket_result.is_ok());
    let get_bucket_response = get_bucket_result.unwrap();
    assert_eq!(get_bucket_response.len(), 1);

    let get_bucket_response_result_data =
        get_bucket_response[0].data.d[0].clone();
    /*
     * All errors should have a "name" and "message" property
     * to comply with the error format in the Fast protocol.
     */
    assert_eq!(
        get_bucket_response_result_data,
        json!({"error": {
            "name": "BucketNotFound",
            "message": "requested bucket not found",
        }})
    );
    let get_bucket_response_result: Result<BucketsMdapiWrappedError, _> =
        serde_json::from_value(get_bucket_response_result_data);
    assert!(get_bucket_response_result.is_ok());
    assert_eq!(
        get_bucket_response_result.unwrap(),
        BucketsMdapiWrappedError::new(BucketsMdapiError::BucketNotFound),
    );

    // Create a bucket
    let create_bucket_payload = bucket::create::CreateBucketPayload {
        owner: owner_id,
        name: bucket.clone(),
        vnode: 0,
        request_id,
    };

    let create_bucket_json =
        serde_json::to_value(vec![create_bucket_payload]).unwrap();
    let create_bucket_fast_msg_data =
        FastMessageData::new("createbucket".into(), create_bucket_json);
    let create_bucket_fast_msg =
        FastMessage::data(msg_id, create_bucket_fast_msg_data);
    let mut create_bucket_result =
        util::handle_msg(&create_bucket_fast_msg, &pool, &metrics, &log);

    assert!(create_bucket_result.is_ok());
    let create_bucket_response = create_bucket_result.unwrap();
    assert_eq!(create_bucket_response.len(), 1);

    let create_bucket_response_result: Result<bucket::BucketResponse, _> =
        serde_json::from_value(create_bucket_response[0].data.d[0].clone());
    assert!(create_bucket_response_result.is_ok());
    assert_eq!(create_bucket_response_result.unwrap().name, bucket);

    // Read bucket and verify success
    get_bucket_result =
        util::handle_msg(&get_bucket_fast_msg, &pool, &metrics, &log);

    assert!(get_bucket_result.is_ok());
    let get_bucket_response = get_bucket_result.unwrap();
    assert_eq!(get_bucket_response.len(), 1);

    let get_bucket_response_result: Result<bucket::BucketResponse, _> =
        serde_json::from_value(get_bucket_response[0].data.d[0].clone());
    assert!(get_bucket_response_result.is_ok());
    assert_eq!(get_bucket_response_result.unwrap().name, bucket);

    // Create same bucket again -> BucketAlreadyExists
    create_bucket_result =
        util::handle_msg(&create_bucket_fast_msg, &pool, &metrics, &log);

    assert!(create_bucket_result.is_ok());
    let create_bucket_response = create_bucket_result.unwrap();
    assert_eq!(create_bucket_response.len(), 1);

    let create_bucket_response_result: Result<BucketsMdapiWrappedError, _> =
        serde_json::from_value(create_bucket_response[0].data.d[0].clone());
    assert!(create_bucket_response_result.is_ok());
    assert_eq!(
        create_bucket_response_result.unwrap(),
        BucketsMdapiWrappedError::new(BucketsMdapiError::BucketAlreadyExists),
    );

    // Delete bucket
    let delete_bucket_payload = bucket::DeleteBucketPayload {
        owner: owner_id,
        name: bucket.clone(),
        vnode: 0,
        request_id,
    };
    let delete_bucket_json =
        serde_json::to_value(vec![delete_bucket_payload]).unwrap();
    let delete_bucket_fast_msg_data =
        FastMessageData::new("deletebucket".into(), delete_bucket_json);
    let delete_bucket_fast_msg =
        FastMessage::data(msg_id, delete_bucket_fast_msg_data);
    let mut delete_bucket_result =
        util::handle_msg(&delete_bucket_fast_msg, &pool, &metrics, &log);

    assert!(delete_bucket_result.is_ok());
    let delete_bucket_response = delete_bucket_result.unwrap();
    assert_eq!(delete_bucket_response.len(), 1);

    let delete_bucket_response_result: Result<u64, _> =
        serde_json::from_value(delete_bucket_response[0].data.d[0].clone());
    assert!(delete_bucket_response_result.is_ok());
    assert_eq!(delete_bucket_response_result.unwrap(), 1);

    // Read bucket again -> BucketNotFound
    get_bucket_result =
        util::handle_msg(&get_bucket_fast_msg, &pool, &metrics, &log);

    assert!(get_bucket_result.is_ok());
    let get_bucket_response = get_bucket_result.unwrap();
    assert_eq!(get_bucket_response.len(), 1);

    let get_bucket_response_result: Result<BucketsMdapiWrappedError, _> =
        serde_json::from_value(get_bucket_response[0].data.d[0].clone());
    assert!(get_bucket_response_result.is_ok());
    assert_eq!(
        get_bucket_response_result.unwrap(),
        BucketsMdapiWrappedError::new(BucketsMdapiError::BucketNotFound),
    );

    // Delete nonexistent bucket -> BucketNotFound
    delete_bucket_result =
        util::handle_msg(&delete_bucket_fast_msg, &pool, &metrics, &log);

    assert!(delete_bucket_result.is_ok());
    let delete_bucket_response = delete_bucket_result.unwrap();
    assert_eq!(delete_bucket_response.len(), 1);

    let delete_bucket_response_result: Result<BucketsMdapiWrappedError, _> =
        serde_json::from_value(delete_bucket_response[0].data.d[0].clone());
    assert!(delete_bucket_response_result.is_ok());
    assert_eq!(
        delete_bucket_response_result.unwrap(),
        BucketsMdapiWrappedError::new(BucketsMdapiError::BucketNotFound),
    );

    // List buckets (empty)
    let list_buckets_payload = bucket::list::ListBucketsPayload {
        owner: owner_id,
        vnode: 0,
        prefix: Some("testbucket".into()),
        limit: 1000,
        marker: None,
        request_id,
    };

    let list_buckets_json =
        serde_json::to_value(vec![list_buckets_payload]).unwrap();
    let list_buckets_fast_msg_data =
        FastMessageData::new("listbuckets".into(), list_buckets_json);
    let list_buckets_fast_msg =
        FastMessage::data(msg_id, list_buckets_fast_msg_data);
    let mut list_buckets_result =
        util::handle_msg(&list_buckets_fast_msg, &pool, &metrics, &log);

    assert!(list_buckets_result.is_ok());
    let list_buckets_response = list_buckets_result.unwrap();
    assert_eq!(list_buckets_response.len(), 0);

    // Create bucket and list again -> 1
    create_bucket_result =
        util::handle_msg(&create_bucket_fast_msg, &pool, &metrics, &log);

    assert!(create_bucket_result.is_ok());
    let create_bucket_response = create_bucket_result.unwrap();
    assert_eq!(create_bucket_response.len(), 1);

    let create_bucket_response_result: Result<bucket::BucketResponse, _> =
        serde_json::from_value(create_bucket_response[0].data.d[0].clone());
    assert!(create_bucket_response_result.is_ok());
    assert_eq!(create_bucket_response_result.unwrap().name, bucket);

    list_buckets_result =
        util::handle_msg(&list_buckets_fast_msg, &pool, &metrics, &log);

    assert!(list_buckets_result.is_ok());
    let list_buckets_response = list_buckets_result.unwrap();
    assert_eq!(list_buckets_response.len(), 1);
}

////////////////////////////////////////////////////////////
// Object CRUD tests
////////////////////////////////////////////////////////////

#[test]
fn verify_object_handlers() {
    setup_test_env!(pool, metrics, log);
    let msg_id: u32 = 0x1;
    let owner_id = Uuid::new_v4();
    let bucket_id = Uuid::new_v4();
    let object: String = "testobject".into();
    let request_id = Uuid::new_v4();

    // Get nonexistent object
    let conditions: conditional::Conditions = Default::default();
    let get_object_payload = object::GetObjectPayload {
        owner: owner_id,
        bucket_id,
        name: object.clone(),
        vnode: 1,
        request_id,
        conditions,
    };

    let get_object_json =
        serde_json::to_value(vec![&get_object_payload]).unwrap();
    let get_object_fast_msg_data =
        FastMessageData::new("getobject".into(), get_object_json);
    let get_object_fast_msg =
        FastMessage::data(msg_id, get_object_fast_msg_data);
    let mut get_object_result =
        util::handle_msg(&get_object_fast_msg, &pool, &metrics, &log);

    assert!(get_object_result.is_ok());
    let get_object_response = get_object_result.unwrap();
    assert_eq!(get_object_response.len(), 1);

    let get_object_response_result: Result<BucketsMdapiWrappedError, _> =
        serde_json::from_value(get_object_response[0].data.d[0].clone());
    assert!(get_object_response_result.is_ok());
    assert_eq!(
        get_object_response_result.unwrap(),
        BucketsMdapiWrappedError::new(BucketsMdapiError::ObjectNotFound),
    );

    // Update nonexistent object
    let object_id = Uuid::new_v4();

    let mut update_headers = HashMap::new();
    let _ = update_headers.insert(
        "m-custom-header1".to_string(),
        Some("customheaderval1".to_string()),
    );
    let _ = update_headers.insert(
        "m-custom-header2".to_string(),
        Some("customheaderval2".to_string()),
    );
    let conditions: conditional::Conditions = Default::default();

    let update_sharks = vec![
        object::StorageNodeIdentifier {
            datacenter: "us-west-1".into(),
            manta_storage_id: "2.stor.us-west.joyent.com".into(),
        },
        object::StorageNodeIdentifier {
            datacenter: "us-west-2".into(),
            manta_storage_id: "4.stor.us-west.joyent.com".into(),
        },
    ];

    let update_object_payload = object::update::UpdateObjectPayload {
        owner: owner_id,
        bucket_id,
        name: object.clone(),
        id: object_id,
        vnode: 1,
        content_type: "text/html".into(),
        headers: update_headers,
        sharks: Some(update_sharks.clone()),
        properties: None,
        request_id,
        conditions,
    };

    let update_object_json =
        serde_json::to_value(vec![update_object_payload]).unwrap();
    let update_object_fast_msg_data =
        FastMessageData::new("updateobject".into(), update_object_json);
    let update_object_fast_msg =
        FastMessage::data(msg_id, update_object_fast_msg_data);
    let mut update_object_result =
        util::handle_msg(&update_object_fast_msg, &pool, &metrics, &log);

    assert!(update_object_result.is_ok());
    let mut update_object_response = update_object_result.unwrap();
    assert_eq!(update_object_response.len(), 1);

    let update_object_response_result: Result<BucketsMdapiWrappedError, _> =
        serde_json::from_value(update_object_response[0].data.d[0].clone());
    assert!(update_object_response_result.is_ok());
    assert_eq!(
        update_object_response_result.unwrap(),
        BucketsMdapiWrappedError::new(BucketsMdapiError::ObjectNotFound),
    );

    // Create object with if-match:"*" -> PreconditionFailed
    let shark1 = object::StorageNodeIdentifier {
        datacenter: "us-east-1".into(),
        manta_storage_id: "1.stor.us-east.joyent.com".into(),
    };
    let shark2 = object::StorageNodeIdentifier {
        datacenter: "us-east-2".into(),
        manta_storage_id: "3.stor.us-east.joyent.com".into(),
    };
    let conditions = serde_json::from_value::<conditional::Conditions>(json!({
        "if-match": [ "*" ]
    }))
    .unwrap();

    let create_object_payload = object::create::CreateObjectPayload {
        owner: owner_id,
        bucket_id,
        name: object.clone(),
        id: object_id,
        vnode: 1,
        content_length: 5,
        content_md5: "xzY5jJbR9rcrMRhlcmi/8g==".into(),
        content_type: "text/plain".into(),
        headers: HashMap::new(),
        sharks: vec![shark1, shark2],
        properties: None,
        request_id,
        conditions,
    };

    let create_object_json =
        serde_json::to_value(vec![create_object_payload]).unwrap();
    let create_object_fast_msg_data =
        FastMessageData::new("createobject".into(), create_object_json);
    let create_object_fast_msg =
        FastMessage::data(msg_id, create_object_fast_msg_data);
    let create_object_result =
        util::handle_msg(&create_object_fast_msg, &pool, &metrics, &log);

    assert!(create_object_result.is_ok());
    let create_object_response = create_object_result.unwrap();
    assert_eq!(create_object_response.len(), 1);

    let create_object_response_result: Result<BucketsMdapiWrappedError, _> =
        serde_json::from_value(create_object_response[0].data.d[0].clone());
    assert!(create_object_response_result.is_ok());
    assert_eq!(
        create_object_response_result.unwrap(),
        BucketsMdapiWrappedError::new(
            BucketsMdapiError::PreconditionFailedError(format!(
                "if-match '\"*\"' matched a non-existent object"
            ))
        ),
    );

    // Create object with if-none-match:"*" -> success
    let shark1 = object::StorageNodeIdentifier {
        datacenter: "us-east-1".into(),
        manta_storage_id: "1.stor.us-east.joyent.com".into(),
    };
    let shark2 = object::StorageNodeIdentifier {
        datacenter: "us-east-2".into(),
        manta_storage_id: "3.stor.us-east.joyent.com".into(),
    };
    let conditions = serde_json::from_value::<conditional::Conditions>(json!({
        "if-none-match": [ "*" ]
    }))
    .unwrap();

    let create_object_payload = object::create::CreateObjectPayload {
        owner: owner_id,
        bucket_id,
        name: object.clone(),
        id: object_id,
        vnode: 1,
        content_length: 5,
        content_md5: "xzY5jJbR9rcrMRhlcmi/8g==".into(),
        content_type: "text/plain".into(),
        headers: HashMap::new(),
        sharks: vec![shark1, shark2],
        properties: None,
        request_id,
        conditions,
    };

    let create_object_json =
        serde_json::to_value(vec![create_object_payload]).unwrap();
    let create_object_fast_msg_data =
        FastMessageData::new("createobject".into(), create_object_json);
    let create_object_fast_msg =
        FastMessage::data(msg_id, create_object_fast_msg_data);
    let create_object_result =
        util::handle_msg(&create_object_fast_msg, &pool, &metrics, &log);

    assert!(create_object_result.is_ok());
    let create_object_response = create_object_result.unwrap();
    assert_eq!(create_object_response.len(), 1);

    let create_object_response_result: Result<object::ObjectResponse, _> =
        serde_json::from_value(create_object_response[0].data.d[0].clone());
    assert!(create_object_response_result.is_ok());
    assert_eq!(create_object_response_result.unwrap().name, object);

    // Create duplicate with if-none-match:"*" ->
    // PreconditionFailed
    let shark1 = object::StorageNodeIdentifier {
        datacenter: "us-east-1".into(),
        manta_storage_id: "1.stor.us-east.joyent.com".into(),
    };
    let shark2 = object::StorageNodeIdentifier {
        datacenter: "us-east-2".into(),
        manta_storage_id: "3.stor.us-east.joyent.com".into(),
    };
    let conditions = serde_json::from_value::<conditional::Conditions>(json!({
        "if-none-match": [ "*" ]
    }))
    .unwrap();

    let create_object_payload = object::create::CreateObjectPayload {
        owner: owner_id,
        bucket_id,
        name: object.clone(),
        id: object_id,
        vnode: 1,
        content_length: 5,
        content_md5: "xzY5jJbR9rcrMRhlcmi/8g==".into(),
        content_type: "text/plain".into(),
        headers: HashMap::new(),
        sharks: vec![shark1, shark2],
        properties: None,
        request_id,
        conditions,
    };

    let create_object_json =
        serde_json::to_value(vec![create_object_payload]).unwrap();
    let create_object_fast_msg_data =
        FastMessageData::new("createobject".into(), create_object_json);
    let create_object_fast_msg =
        FastMessage::data(msg_id, create_object_fast_msg_data);
    let create_object_result =
        util::handle_msg(&create_object_fast_msg, &pool, &metrics, &log);

    assert!(create_object_result.is_ok());
    let create_object_response = create_object_result.unwrap();
    assert_eq!(create_object_response.len(), 1);

    let create_object_response_result: Result<BucketsMdapiWrappedError, _> =
        serde_json::from_value(create_object_response[0].data.d[0].clone());
    assert!(create_object_response_result.is_ok());
    assert_eq!(
        create_object_response_result.unwrap(),
        BucketsMdapiWrappedError::new(
            BucketsMdapiError::PreconditionFailedError(format!(
                "if-none-match '\"*\"' matched etag '{}'",
                object_id
            ))
        ),
    );

    // Read object -> success
    get_object_result =
        util::handle_msg(&get_object_fast_msg, &pool, &metrics, &log);

    assert!(get_object_result.is_ok());
    let get_object_response = get_object_result.unwrap();
    assert_eq!(get_object_response.len(), 1);

    let get_object_response_result: Result<object::ObjectResponse, _> =
        serde_json::from_value(get_object_response[0].data.d[0].clone());
    assert!(get_object_response_result.is_ok());
    let mut get_object_unwrapped_result = get_object_response_result.unwrap();
    assert_eq!(get_object_unwrapped_result.name, object);
    assert_eq!(&get_object_unwrapped_result.content_type, "text/plain");

    // Update object metadata -> success
    update_object_result =
        util::handle_msg(&update_object_fast_msg, &pool, &metrics, &log);

    assert!(update_object_result.is_ok());
    update_object_response = update_object_result.unwrap();
    assert_eq!(update_object_response.len(), 1);

    let update_object_response_result: Result<object::ObjectResponse, _> =
        serde_json::from_value(update_object_response[0].data.d[0].clone());
    assert!(update_object_response_result.is_ok());
    let update_object_unwrapped_result = update_object_response_result.unwrap();
    assert_eq!(update_object_unwrapped_result.name, object);
    assert_eq!(&update_object_unwrapped_result.content_type, "text/html");
    // Verify sharks were persisted by the update
    assert_eq!(update_object_unwrapped_result.sharks, update_sharks);

    // Read object -> verify update persisted
    get_object_result =
        util::handle_msg(&get_object_fast_msg, &pool, &metrics, &log);
    assert!(get_object_result.is_ok());
    let get_object_response = get_object_result.unwrap();
    assert_eq!(get_object_response.len(), 1);

    let get_object_response_result: Result<object::ObjectResponse, _> =
        serde_json::from_value(get_object_response[0].data.d[0].clone());
    assert!(get_object_response_result.is_ok());
    get_object_unwrapped_result = get_object_response_result.unwrap();
    assert_eq!(get_object_unwrapped_result.name, object);
    assert_eq!(&get_object_unwrapped_result.content_type, "text/html");

    // Get with if-match: correct ETag -> success
    let request_id = Uuid::new_v4();

    let conditions = serde_json::from_value::<conditional::Conditions>(json!({
        "if-match": [ object_id.to_string() ],
    }))
    .unwrap();

    let get_object_payload = object::GetObjectPayload {
        owner: owner_id,
        bucket_id,
        name: object.clone(),
        vnode: 1,
        request_id,
        conditions,
    };

    let get_object_json =
        serde_json::to_value(vec![&get_object_payload]).unwrap();
    let get_object_fast_msg_data =
        FastMessageData::new("getobject".into(), get_object_json);
    let get_object_fast_msg =
        FastMessage::data(msg_id, get_object_fast_msg_data);
    get_object_result =
        util::handle_msg(&get_object_fast_msg, &pool, &metrics, &log);

    assert!(get_object_result.is_ok());
    let get_object_response = get_object_result.unwrap();
    assert_eq!(get_object_response.len(), 1);

    let get_object_response_result: Result<object::ObjectResponse, _> =
        serde_json::from_value(get_object_response[0].data.d[0].clone());
    assert!(get_object_response_result.is_ok());
    get_object_unwrapped_result = get_object_response_result.unwrap();
    assert_eq!(get_object_unwrapped_result.name, object);
    assert_eq!(&get_object_unwrapped_result.content_type, "text/html");

    // Get with if-match: wrong ETag -> PreconditionFailed
    let request_id = Uuid::new_v4();

    let if_match_etag = Uuid::new_v4();
    let conditions = serde_json::from_value::<conditional::Conditions>(json!({
        "if-match": [ if_match_etag ],
    }))
    .unwrap();

    let mut get_object_payload = object::GetObjectPayload {
        owner: owner_id,
        bucket_id,
        name: object.clone(),
        vnode: 1,
        request_id,
        conditions,
    };

    let get_object_json =
        serde_json::to_value(vec![&get_object_payload]).unwrap();
    let get_object_fast_msg_data =
        FastMessageData::new("getobject".into(), get_object_json);
    let get_object_fast_msg =
        FastMessage::data(msg_id, get_object_fast_msg_data);
    get_object_result =
        util::handle_msg(&get_object_fast_msg, &pool, &metrics, &log);

    assert!(get_object_result.is_ok());
    let get_object_response = get_object_result.unwrap();
    assert_eq!(get_object_response.len(), 1);

    let get_object_response_result: Result<BucketsMdapiWrappedError, _> =
        serde_json::from_value(get_object_response[0].data.d[0].clone());
    assert!(get_object_response_result.is_ok());
    assert_eq!(
        get_object_response_result.unwrap(),
        BucketsMdapiWrappedError::new(
            BucketsMdapiError::PreconditionFailedError(format!(
                "if-match '\"{}\"' didn't match etag '{}'",
                if_match_etag, object_id
            ))
        ),
    );

    // Delete object
    get_object_payload.conditions = Default::default();
    let delete_object_json =
        serde_json::to_value(vec![get_object_payload]).unwrap();
    let delete_object_fast_msg_data =
        FastMessageData::new("deleteobject".into(), delete_object_json);
    let delete_object_fast_msg =
        FastMessage::data(msg_id, delete_object_fast_msg_data);
    let mut delete_object_result =
        util::handle_msg(&delete_object_fast_msg, &pool, &metrics, &log);

    assert!(delete_object_result.is_ok());
    let delete_object_response = delete_object_result.unwrap();
    assert_eq!(delete_object_response.len(), 1);

    let delete_object_response_result: Result<
        Vec<object::DeleteObjectResponse>,
        _,
    > = serde_json::from_value(delete_object_response[0].data.d[0].clone());
    assert!(delete_object_response_result.is_ok());
    let delete_object_response = delete_object_response_result.unwrap();
    assert_eq!(delete_object_response.len(), 1);
    assert_eq!(&delete_object_response[0].owner, &owner_id);
    assert_eq!(&delete_object_response[0].bucket_id, &bucket_id);
    assert_eq!(&delete_object_response[0].name, &object);

    // Read deleted object -> ObjectNotFound
    get_object_result =
        util::handle_msg(&get_object_fast_msg, &pool, &metrics, &log);

    assert!(get_object_result.is_ok());
    let get_object_response = get_object_result.unwrap();
    assert_eq!(get_object_response.len(), 1);

    let get_object_response_result: Result<BucketsMdapiWrappedError, _> =
        serde_json::from_value(get_object_response[0].data.d[0].clone());
    assert!(get_object_response_result.is_ok());
    assert_eq!(
        get_object_response_result.unwrap(),
        BucketsMdapiWrappedError::new(BucketsMdapiError::ObjectNotFound),
    );

    // Delete nonexistent object -> ObjectNotFound
    delete_object_result =
        util::handle_msg(&delete_object_fast_msg, &pool, &metrics, &log);

    assert!(delete_object_result.is_ok());
    let delete_object_response = delete_object_result.unwrap();
    assert_eq!(delete_object_response.len(), 1);

    let delete_object_response_result: Result<BucketsMdapiWrappedError, _> =
        serde_json::from_value(delete_object_response[0].data.d[0].clone());
    assert!(delete_object_response_result.is_ok());
    assert_eq!(
        delete_object_response_result.unwrap(),
        BucketsMdapiWrappedError::new(BucketsMdapiError::ObjectNotFound),
    );

    // List objects (empty)
    let list_objects_payload = object::list::ListObjectsPayload {
        owner: owner_id,
        bucket_id,
        vnode: 1,
        prefix: Some("testobject".into()),
        limit: 1000,
        marker: None,
        request_id,
    };

    let list_objects_json =
        serde_json::to_value(vec![list_objects_payload]).unwrap();
    let list_objects_fast_msg_data =
        FastMessageData::new("listobjects".into(), list_objects_json);
    let list_objects_fast_msg =
        FastMessage::data(msg_id, list_objects_fast_msg_data);
    let mut list_objects_result =
        util::handle_msg(&list_objects_fast_msg, &pool, &metrics, &log);

    assert!(list_objects_result.is_ok());
    let list_objects_response = list_objects_result.unwrap();
    assert_eq!(list_objects_response.len(), 0);

    // Create object and list again -> 1
    let create_object_result =
        util::handle_msg(&create_object_fast_msg, &pool, &metrics, &log);

    assert!(create_object_result.is_ok());
    let create_object_response = create_object_result.unwrap();
    assert_eq!(create_object_response.len(), 1);

    let create_object_response_result: Result<object::ObjectResponse, _> =
        serde_json::from_value(create_object_response[0].data.d[0].clone());
    assert!(create_object_response_result.is_ok());
    assert_eq!(create_object_response_result.unwrap().name, object);

    list_objects_result =
        util::handle_msg(&list_objects_fast_msg, &pool, &metrics, &log);

    assert!(list_objects_result.is_ok());
    let list_objects_response = list_objects_result.unwrap();
    assert_eq!(list_objects_response.len(), 1);
}

////////////////////////////////////////////////////////////
// Batch update tests
////////////////////////////////////////////////////////////

#[test]
fn verify_batch_update_handlers() {
    setup_test_env!(pool, metrics, log);
    let msg_id: u32 = 0x1;
    let owner_id = Uuid::new_v4();
    let bucket_id = Uuid::new_v4();
    let object: String = "testobject".into();
    let object_id = Uuid::new_v4();
    let request_id = Uuid::new_v4();

    // Setup: create a bucket and an object
    create_test_bucket!(
        msg_id,
        owner_id,
        "testbucket",
        0,
        request_id,
        pool,
        metrics,
        log
    );
    create_test_object!(
        msg_id, owner_id, bucket_id, &object, object_id, 1, request_id, pool,
        metrics, log
    );

    // Sharks used for batch update payloads
    let batch_sharks = vec![
        object::StorageNodeIdentifier {
            datacenter: "us-west-1".into(),
            manta_storage_id: "2.stor.us-west.joyent.com".into(),
        },
        object::StorageNodeIdentifier {
            datacenter: "us-west-2".into(),
            manta_storage_id: "4.stor.us-west.joyent.com".into(),
        },
    ];

    // Batch update nonexistent object
    let nonexistent_id = Uuid::new_v4();
    let request_id = Uuid::new_v4();
    let batch_update_payload =
        object::batch_update::BatchUpdateObjectsPayload {
            objects: vec![object::update::UpdateObjectPayload {
                owner: owner_id,
                bucket_id,
                name: "no_such_object".into(),
                id: nonexistent_id,
                vnode: 1,
                content_type: "text/plain".into(),
                headers: HashMap::new(),
                sharks: Some(batch_sharks.clone()),
                properties: None,
                request_id,
                conditions: Default::default(),
            }],
            request_id,
        };

    let batch_update_json =
        serde_json::to_value(vec![&batch_update_payload]).unwrap();
    let batch_update_fast_msg_data =
        FastMessageData::new("batchupdateobjects".into(), batch_update_json);
    let batch_update_fast_msg =
        FastMessage::data(msg_id, batch_update_fast_msg_data);
    let batch_update_result =
        util::handle_msg(&batch_update_fast_msg, &pool, &metrics, &log);

    assert!(batch_update_result.is_ok());
    let batch_update_response = batch_update_result.unwrap();
    assert_eq!(batch_update_response.len(), 1);

    let batch_resp: object::batch_update::BatchUpdateObjectsResponse =
        serde_json::from_value(batch_update_response[0].data.d[0].clone())
            .unwrap();
    assert!(!batch_resp.is_success());
    assert_eq!(batch_resp.failed_vnodes.len(), 1);
    assert_eq!(batch_resp.failed_count(), 1);

    // Batch update happy path
    let request_id = Uuid::new_v4();
    let mut batch_headers = HashMap::new();
    let _ = batch_headers
        .insert("m-batch-header".to_string(), Some("batchval".to_string()));

    let batch_update_payload =
        object::batch_update::BatchUpdateObjectsPayload {
            objects: vec![object::update::UpdateObjectPayload {
                owner: owner_id,
                bucket_id,
                name: object.clone(),
                id: object_id,
                vnode: 1,
                content_type: "application/json".into(),
                headers: batch_headers,
                sharks: Some(batch_sharks.clone()),
                properties: None,
                request_id,
                conditions: Default::default(),
            }],
            request_id,
        };

    let batch_update_json =
        serde_json::to_value(vec![&batch_update_payload]).unwrap();
    let batch_update_fast_msg_data =
        FastMessageData::new("batchupdateobjects".into(), batch_update_json);
    let batch_update_fast_msg =
        FastMessage::data(msg_id, batch_update_fast_msg_data);
    let batch_update_result =
        util::handle_msg(&batch_update_fast_msg, &pool, &metrics, &log);

    assert!(batch_update_result.is_ok());
    let batch_update_response = batch_update_result.unwrap();
    assert_eq!(batch_update_response.len(), 1);

    let batch_resp: object::batch_update::BatchUpdateObjectsResponse =
        serde_json::from_value(batch_update_response[0].data.d[0].clone())
            .unwrap();
    assert!(batch_resp.is_success());

    // Verify persistence via getobject
    let request_id = Uuid::new_v4();
    let conditions: conditional::Conditions = Default::default();
    let get_object_payload = object::GetObjectPayload {
        owner: owner_id,
        bucket_id,
        name: object.clone(),
        vnode: 1,
        request_id,
        conditions,
    };

    let get_object_json =
        serde_json::to_value(vec![&get_object_payload]).unwrap();
    let get_object_fast_msg_data =
        FastMessageData::new("getobject".into(), get_object_json);
    let get_object_fast_msg =
        FastMessage::data(msg_id, get_object_fast_msg_data);
    let get_object_result =
        util::handle_msg(&get_object_fast_msg, &pool, &metrics, &log);

    assert!(get_object_result.is_ok());
    let get_object_response = get_object_result.unwrap();
    assert_eq!(get_object_response.len(), 1);

    let get_object_response_result: Result<object::ObjectResponse, _> =
        serde_json::from_value(get_object_response[0].data.d[0].clone());
    assert!(get_object_response_result.is_ok());
    let get_object_after_batch = get_object_response_result.unwrap();
    assert_eq!(get_object_after_batch.name, object);
    assert_eq!(get_object_after_batch.content_type, "application/json");
    // Verify sharks were persisted by the batch update
    assert_eq!(get_object_after_batch.sharks, batch_sharks);

    // Batch update with correct if-match ETag
    let request_id = Uuid::new_v4();
    let conditions = serde_json::from_value::<conditional::Conditions>(
        json!({ "if-match": [ object_id.to_string() ] }),
    )
    .unwrap();

    let batch_update_payload =
        object::batch_update::BatchUpdateObjectsPayload {
            objects: vec![object::update::UpdateObjectPayload {
                owner: owner_id,
                bucket_id,
                name: object.clone(),
                id: object_id,
                vnode: 1,
                content_type: "text/csv".into(),
                headers: HashMap::new(),
                sharks: Some(batch_sharks.clone()),
                properties: None,
                request_id,
                conditions,
            }],
            request_id,
        };

    let batch_update_json =
        serde_json::to_value(vec![&batch_update_payload]).unwrap();
    let batch_update_fast_msg_data =
        FastMessageData::new("batchupdateobjects".into(), batch_update_json);
    let batch_update_fast_msg =
        FastMessage::data(msg_id, batch_update_fast_msg_data);
    let batch_update_result =
        util::handle_msg(&batch_update_fast_msg, &pool, &metrics, &log);

    assert!(batch_update_result.is_ok());
    let batch_update_response = batch_update_result.unwrap();
    assert_eq!(batch_update_response.len(), 1);

    let batch_resp: object::batch_update::BatchUpdateObjectsResponse =
        serde_json::from_value(batch_update_response[0].data.d[0].clone())
            .unwrap();
    assert!(batch_resp.is_success());

    // Batch update with wrong if-match ETag
    let request_id = Uuid::new_v4();
    let wrong_etag = Uuid::new_v4();
    let conditions = serde_json::from_value::<conditional::Conditions>(json!({
        "if-match": [ wrong_etag.to_string() ]
    }))
    .unwrap();

    let batch_update_payload =
        object::batch_update::BatchUpdateObjectsPayload {
            objects: vec![object::update::UpdateObjectPayload {
                owner: owner_id,
                bucket_id,
                name: object.clone(),
                id: object_id,
                vnode: 1,
                content_type: "text/xml".into(),
                headers: HashMap::new(),
                sharks: Some(batch_sharks.clone()),
                properties: None,
                request_id,
                conditions,
            }],
            request_id,
        };

    let batch_update_json =
        serde_json::to_value(vec![&batch_update_payload]).unwrap();
    let batch_update_fast_msg_data =
        FastMessageData::new("batchupdateobjects".into(), batch_update_json);
    let batch_update_fast_msg =
        FastMessage::data(msg_id, batch_update_fast_msg_data);
    let batch_update_result =
        util::handle_msg(&batch_update_fast_msg, &pool, &metrics, &log);

    assert!(batch_update_result.is_ok());
    let batch_update_response = batch_update_result.unwrap();
    assert_eq!(batch_update_response.len(), 1);

    let batch_resp: object::batch_update::BatchUpdateObjectsResponse =
        serde_json::from_value(batch_update_response[0].data.d[0].clone())
            .unwrap();
    assert!(!batch_resp.is_success());
    assert_eq!(batch_resp.failed_vnodes.len(), 1);
    assert_eq!(batch_resp.failed_count(), 1);

    // Multi-object batch update (same vnode)
    // Create a second object on vnode 1
    let object_b: String = "testobject_b".into();
    let object_b_id = Uuid::new_v4();
    let request_id = Uuid::new_v4();
    create_test_object!(
        msg_id,
        owner_id,
        bucket_id,
        &object_b,
        object_b_id,
        1,
        request_id,
        pool,
        metrics,
        log
    );

    let request_id = Uuid::new_v4();
    let batch_update_payload =
        object::batch_update::BatchUpdateObjectsPayload {
            objects: vec![
                object::update::UpdateObjectPayload {
                    owner: owner_id,
                    bucket_id,
                    name: object.clone(),
                    id: object_id,
                    vnode: 1,
                    content_type: "multi/obj-a".into(),
                    headers: HashMap::new(),
                    sharks: Some(batch_sharks.clone()),
                    properties: None,
                    request_id,
                    conditions: Default::default(),
                },
                object::update::UpdateObjectPayload {
                    owner: owner_id,
                    bucket_id,
                    name: object_b.clone(),
                    id: object_b_id,
                    vnode: 1,
                    content_type: "multi/obj-b".into(),
                    headers: HashMap::new(),
                    sharks: Some(batch_sharks.clone()),
                    properties: None,
                    request_id,
                    conditions: Default::default(),
                },
            ],
            request_id,
        };

    let batch_update_json =
        serde_json::to_value(vec![&batch_update_payload]).unwrap();
    let batch_update_fast_msg_data =
        FastMessageData::new("batchupdateobjects".into(), batch_update_json);
    let batch_update_fast_msg =
        FastMessage::data(msg_id, batch_update_fast_msg_data);
    let batch_update_result =
        util::handle_msg(&batch_update_fast_msg, &pool, &metrics, &log);

    assert!(batch_update_result.is_ok());
    let batch_update_response = batch_update_result.unwrap();
    assert_eq!(batch_update_response.len(), 1);

    let batch_resp: object::batch_update::BatchUpdateObjectsResponse =
        serde_json::from_value(batch_update_response[0].data.d[0].clone())
            .unwrap();
    assert!(batch_resp.is_success());

    // Multi-vnode batch update
    // Create a third object on vnode 0 (different vnode)
    let object_c: String = "testobject_c".into();
    let object_c_id = Uuid::new_v4();
    let request_id = Uuid::new_v4();
    create_test_object!(
        msg_id,
        owner_id,
        bucket_id,
        &object_c,
        object_c_id,
        0,
        request_id,
        pool,
        metrics,
        log
    );

    let request_id = Uuid::new_v4();
    let batch_update_payload =
        object::batch_update::BatchUpdateObjectsPayload {
            objects: vec![
                object::update::UpdateObjectPayload {
                    owner: owner_id,
                    bucket_id,
                    name: object.clone(),
                    id: object_id,
                    vnode: 1,
                    content_type: "vnode1/updated".into(),
                    headers: HashMap::new(),
                    sharks: Some(batch_sharks.clone()),
                    properties: None,
                    request_id,
                    conditions: Default::default(),
                },
                object::update::UpdateObjectPayload {
                    owner: owner_id,
                    bucket_id,
                    name: object_c.clone(),
                    id: object_c_id,
                    vnode: 0,
                    content_type: "vnode0/updated".into(),
                    headers: HashMap::new(),
                    sharks: Some(batch_sharks.clone()),
                    properties: None,
                    request_id,
                    conditions: Default::default(),
                },
            ],
            request_id,
        };

    let batch_update_json =
        serde_json::to_value(vec![&batch_update_payload]).unwrap();
    let batch_update_fast_msg_data =
        FastMessageData::new("batchupdateobjects".into(), batch_update_json);
    let batch_update_fast_msg =
        FastMessage::data(msg_id, batch_update_fast_msg_data);
    let batch_update_result =
        util::handle_msg(&batch_update_fast_msg, &pool, &metrics, &log);

    assert!(batch_update_result.is_ok());
    let batch_update_response = batch_update_result.unwrap();
    assert_eq!(batch_update_response.len(), 1);

    let batch_resp: object::batch_update::BatchUpdateObjectsResponse =
        serde_json::from_value(batch_update_response[0].data.d[0].clone())
            .unwrap();
    assert!(batch_resp.is_success());

    // Cross-vnode partial failure: vnode 1 succeeds,
    // vnode 0 fails (nonexistent object)
    let request_id = Uuid::new_v4();
    let batch_update_payload =
        object::batch_update::BatchUpdateObjectsPayload {
            objects: vec![
                object::update::UpdateObjectPayload {
                    owner: owner_id,
                    bucket_id,
                    name: object.clone(),
                    id: object_id,
                    vnode: 1,
                    content_type: "should/succeed".into(),
                    headers: HashMap::new(),
                    sharks: Some(batch_sharks.clone()),
                    properties: None,
                    request_id,
                    conditions: Default::default(),
                },
                object::update::UpdateObjectPayload {
                    owner: owner_id,
                    bucket_id,
                    name: "no_such_object_on_v0".into(),
                    id: Uuid::new_v4(),
                    vnode: 0,
                    content_type: "should/fail".into(),
                    headers: HashMap::new(),
                    sharks: Some(batch_sharks.clone()),
                    properties: None,
                    request_id,
                    conditions: Default::default(),
                },
            ],
            request_id,
        };

    let batch_update_json =
        serde_json::to_value(vec![&batch_update_payload]).unwrap();
    let batch_update_fast_msg_data =
        FastMessageData::new("batchupdateobjects".into(), batch_update_json);
    let batch_update_fast_msg =
        FastMessage::data(msg_id, batch_update_fast_msg_data);
    let batch_update_result =
        util::handle_msg(&batch_update_fast_msg, &pool, &metrics, &log);

    assert!(batch_update_result.is_ok());
    let batch_update_response = batch_update_result.unwrap();
    assert_eq!(batch_update_response.len(), 1);

    let batch_resp: object::batch_update::BatchUpdateObjectsResponse =
        serde_json::from_value(batch_update_response[0].data.d[0].clone())
            .unwrap();
    assert!(!batch_resp.is_success());
    // Vnode 0 failed as an atomic unit
    assert_eq!(batch_resp.failed_vnodes.len(), 1);
    assert_eq!(batch_resp.failed_vnodes[0].vnode, 0);
    assert_eq!(batch_resp.failed_vnodes[0].objects.len(), 1);
}

////////////////////////////////////////////////////////////
// Randomized batch update test
////////////////////////////////////////////////////////////

#[test]
fn verify_batch_update_random_workload() {
    setup_test_env!(pool, metrics, log);
    let msg_id: u32 = 0x1;
    let owner_id = Uuid::new_v4();
    let bucket_id = Uuid::new_v4();
    let request_id = Uuid::new_v4();

    // Create a bucket on each vnode
    create_test_bucket!(
        msg_id,
        owner_id,
        "randombucket",
        0,
        request_id,
        pool,
        metrics,
        log
    );

    // Generate 10 objects spread across vnodes 0 and 1.
    // Uuid::new_v4() provides randomness for unique names.
    let obj_count = 10;

    struct TestObj {
        name: String,
        id: Uuid,
        vnode: u64,
    }

    let mut objects: Vec<TestObj> = Vec::new();
    for i in 0..obj_count {
        let name = format!("rnd_obj_{}", Uuid::new_v4());
        let id = Uuid::new_v4();
        let vnode = if i % 2 == 0 { 0 } else { 1 };
        objects.push(TestObj { name, id, vnode });
    }

    // Create all objects in the database
    for obj in &objects {
        create_test_object!(
            msg_id, owner_id, bucket_id, &obj.name, obj.id, obj.vnode,
            request_id, pool, metrics, log
        );
    }

    // Build a batch update with unique content types
    let request_id = Uuid::new_v4();
    let update_payloads: Vec<object::update::UpdateObjectPayload> = objects
        .iter()
        .enumerate()
        .map(|(i, obj)| {
            let content_type = format!("random/type-{}", i);
            let mut headers = HashMap::new();
            let _ = headers.insert(
                format!("m-hdr-{}", Uuid::new_v4()),
                Some(format!("val-{}", Uuid::new_v4())),
            );
            let sharks = vec![object::StorageNodeIdentifier {
                datacenter: format!("dc-{}", i),
                manta_storage_id: format!("{}.stor.dc-{}.joyent.com", i, i),
            }];
            object::update::UpdateObjectPayload {
                owner: owner_id,
                bucket_id,
                name: obj.name.clone(),
                id: obj.id,
                vnode: obj.vnode,
                content_type,
                headers,
                sharks: Some(sharks),
                properties: None,
                request_id,
                conditions: Default::default(),
            }
        })
        .collect();

    let batch_update_payload =
        object::batch_update::BatchUpdateObjectsPayload {
            objects: update_payloads,
            request_id,
        };

    let batch_update_json =
        serde_json::to_value(vec![&batch_update_payload]).unwrap();
    let batch_update_fast_msg_data =
        FastMessageData::new("batchupdateobjects".into(), batch_update_json);
    let batch_update_fast_msg =
        FastMessage::data(msg_id, batch_update_fast_msg_data);
    let batch_update_result =
        util::handle_msg(&batch_update_fast_msg, &pool, &metrics, &log);

    assert!(batch_update_result.is_ok());
    let batch_update_response = batch_update_result.unwrap();
    assert_eq!(batch_update_response.len(), 1);

    let batch_resp: object::batch_update::BatchUpdateObjectsResponse =
        serde_json::from_value(batch_update_response[0].data.d[0].clone())
            .unwrap();
    assert!(batch_resp.is_success());
}

////////////////////////////////////////////////////////////
// Garbage collection tests
////////////////////////////////////////////////////////////

#[test]
fn verify_gc_handlers() {
    setup_test_env!(pool, metrics, log);
    let msg_id: u32 = 0x1;
    let owner_id = Uuid::new_v4();
    let bucket_id = Uuid::new_v4();
    let object: String = "testobject".into();
    let object_id = Uuid::new_v4();
    let request_id = Uuid::new_v4();

    // Setup: create bucket, create object, then delete it
    // to produce garbage records.
    create_test_bucket!(
        msg_id,
        owner_id,
        "testbucket",
        0,
        request_id,
        pool,
        metrics,
        log
    );
    create_test_object!(
        msg_id, owner_id, bucket_id, &object, object_id, 1, request_id, pool,
        metrics, log
    );
    delete_test_object!(
        msg_id, owner_id, bucket_id, &object, 1, request_id, pool, metrics, log
    );

    // Get garbage batch
    let request_id = Uuid::new_v4();
    let mut get_garbage_payload = gc::get::GetGarbagePayload { request_id };

    let mut get_garbage_json =
        serde_json::to_value(vec![&get_garbage_payload]).unwrap();
    let mut get_garbage_fast_msg_data =
        FastMessageData::new("getgcbatch".into(), get_garbage_json);
    let mut get_garbage_fast_msg =
        FastMessage::data(msg_id, get_garbage_fast_msg_data);
    let mut get_garbage_result =
        util::handle_msg(&get_garbage_fast_msg, &pool, &metrics, &log);

    assert!(get_garbage_result.is_ok());
    let mut get_garbage_response = get_garbage_result.unwrap();
    assert_eq!(get_garbage_response.len(), 1);

    let mut get_garbage_response_result: Result<
        gc::get::GetGarbageResponse,
        _,
    > = serde_json::from_value(get_garbage_response[0].data.d[0].clone());

    assert!(get_garbage_response_result.is_ok());
    let mut get_garbage_unwrapped_result = get_garbage_response_result.unwrap();
    assert!(get_garbage_unwrapped_result.batch_id.is_some());
    assert!(!get_garbage_unwrapped_result.garbage.is_empty());

    let batch_id = get_garbage_unwrapped_result.batch_id.unwrap();

    // Get garbage batch again -> same batch_id
    let request_id = Uuid::new_v4();
    get_garbage_payload = gc::get::GetGarbagePayload { request_id };

    get_garbage_json =
        serde_json::to_value(vec![&get_garbage_payload]).unwrap();
    get_garbage_fast_msg_data =
        FastMessageData::new("getgcbatch".into(), get_garbage_json);
    get_garbage_fast_msg = FastMessage::data(msg_id, get_garbage_fast_msg_data);
    get_garbage_result =
        util::handle_msg(&get_garbage_fast_msg, &pool, &metrics, &log);

    assert!(get_garbage_result.is_ok());
    get_garbage_response = get_garbage_result.unwrap();
    assert_eq!(get_garbage_response.len(), 1);

    get_garbage_response_result =
        serde_json::from_value(get_garbage_response[0].data.d[0].clone());

    assert!(get_garbage_response_result.is_ok());
    get_garbage_unwrapped_result = get_garbage_response_result.unwrap();
    assert!(get_garbage_unwrapped_result.batch_id.is_some());
    assert!(!get_garbage_unwrapped_result.garbage.is_empty());

    assert_eq!(batch_id, get_garbage_unwrapped_result.batch_id.unwrap());

    // Delete garbage with wrong batch_id
    let mut delete_garbage_payload = gc::delete::DeleteGarbagePayload {
        batch_id: Uuid::new_v4(),
        request_id,
    };
    let mut delete_garbage_json =
        serde_json::to_value(vec![delete_garbage_payload]).unwrap();
    let mut delete_garbage_fast_msg_data =
        FastMessageData::new("deletegcbatch".into(), delete_garbage_json);
    let mut delete_garbage_fast_msg =
        FastMessage::data(msg_id, delete_garbage_fast_msg_data);
    let mut delete_garbage_result =
        util::handle_msg(&delete_garbage_fast_msg, &pool, &metrics, &log);

    assert!(delete_garbage_result.is_ok());
    let mut delete_garbage_responses = delete_garbage_result.unwrap();
    assert_eq!(delete_garbage_responses.len(), 1);

    let mut delete_garbage_response_result: Result<String, _> =
        serde_json::from_value(delete_garbage_responses[0].data.d[0].clone());
    assert!(delete_garbage_response_result.is_ok());
    let mut delete_garbage_response = delete_garbage_response_result.unwrap();
    assert_eq!(&delete_garbage_response, "ok");

    // Delete garbage with correct batch_id
    delete_garbage_payload = gc::delete::DeleteGarbagePayload {
        batch_id,
        request_id,
    };
    delete_garbage_json =
        serde_json::to_value(vec![delete_garbage_payload]).unwrap();
    delete_garbage_fast_msg_data =
        FastMessageData::new("deletegcbatch".into(), delete_garbage_json);
    delete_garbage_fast_msg =
        FastMessage::data(msg_id, delete_garbage_fast_msg_data);
    delete_garbage_result =
        util::handle_msg(&delete_garbage_fast_msg, &pool, &metrics, &log);

    assert!(delete_garbage_result.is_ok());
    delete_garbage_responses = delete_garbage_result.unwrap();
    assert_eq!(delete_garbage_responses.len(), 1);

    delete_garbage_response_result =
        serde_json::from_value(delete_garbage_responses[0].data.d[0].clone());
    assert!(delete_garbage_response_result.is_ok());
    delete_garbage_response = delete_garbage_response_result.unwrap();
    assert_eq!(&delete_garbage_response, "ok");

    // Get garbage batch -> empty
    get_garbage_result =
        util::handle_msg(&get_garbage_fast_msg, &pool, &metrics, &log);

    assert!(get_garbage_result.is_ok());
    let get_garbage_response = get_garbage_result.unwrap();
    assert_eq!(get_garbage_response.len(), 1);

    let get_garbage_response_result: Result<gc::get::GetGarbageResponse, _> =
        serde_json::from_value(get_garbage_response[0].data.d[0].clone());

    assert!(get_garbage_response_result.is_ok());
    get_garbage_unwrapped_result = get_garbage_response_result.unwrap();
    assert!(get_garbage_unwrapped_result.batch_id.is_none());
    assert!(get_garbage_unwrapped_result.garbage.is_empty());
}

////////////////////////////////////////////////////////////
// Discovery RPC tests
////////////////////////////////////////////////////////////

// Helper: call listvnodes and return the parsed response
fn do_listvnodes(
    msg_id: u32,
    pool: &ConnectionPool<
        PostgresConnection,
        impl cueball::resolver::Resolver,
        impl FnMut(&cueball::backend::Backend) -> PostgresConnection
            + Send
            + 'static,
    >,
    metrics: &RegisteredMetrics,
    log: &Logger,
) -> discovery::ListVnodesResponse {
    let payload = discovery::ListVnodesPayload {
        request_id: Uuid::new_v4(),
    };
    let json = serde_json::to_value(vec![&payload]).unwrap();
    let msg_data = FastMessageData::new("listvnodes".into(), json);
    let msg = FastMessage::data(msg_id, msg_data);
    let result = util::handle_msg(&msg, pool, metrics, log);
    assert!(result.is_ok());
    let response = result.unwrap();
    assert_eq!(response.len(), 1);
    serde_json::from_value(response[0].data.d[0].clone())
        .expect("failed to parse ListVnodesResponse")
}

// Helper: call listowners for a vnode and return the parsed response
fn do_listowners(
    msg_id: u32,
    vnode: u64,
    pool: &ConnectionPool<
        PostgresConnection,
        impl cueball::resolver::Resolver,
        impl FnMut(&cueball::backend::Backend) -> PostgresConnection
            + Send
            + 'static,
    >,
    metrics: &RegisteredMetrics,
    log: &Logger,
) -> discovery::ListOwnersResponse {
    let payload = discovery::ListOwnersPayload {
        vnode,
        request_id: Uuid::new_v4(),
    };
    let json = serde_json::to_value(vec![&payload]).unwrap();
    let msg_data = FastMessageData::new("listowners".into(), json);
    let msg = FastMessage::data(msg_id, msg_data);
    let result = util::handle_msg(&msg, pool, metrics, log);
    assert!(result.is_ok());
    let response = result.unwrap();
    assert_eq!(response.len(), 1);
    serde_json::from_value(response[0].data.d[0].clone())
        .expect("failed to parse ListOwnersResponse")
}

#[test]
fn verify_discovery_handlers() {
    setup_test_env!(pool, metrics, log);
    let msg_id: u32 = 0x1;

    // listvnodes: the test harness creates schemas for vnodes 0 and 1
    let vnodes_resp = do_listvnodes(msg_id, &pool, &metrics, &log);
    assert!(
        vnodes_resp.vnodes.contains(&0),
        "expected vnode 0 in response"
    );
    assert!(
        vnodes_resp.vnodes.contains(&1),
        "expected vnode 1 in response"
    );

    // listowners on both vnodes before any data -> empty
    for vnode in &[0u64, 1] {
        let owners_resp = do_listowners(msg_id, *vnode, &pool, &metrics, &log);
        assert!(
            owners_resp.owners.is_empty(),
            "expected empty owners on vnode {} with no buckets",
            vnode
        );
    }

    // Create buckets for 3 random owners spread across both vnodes.
    // owner_a: buckets on vnode 0 only
    // owner_b: buckets on vnode 1 only
    // owner_c: buckets on both vnodes
    let owner_a = Uuid::new_v4();
    let owner_b = Uuid::new_v4();
    let owner_c = Uuid::new_v4();

    // Use random bucket names to avoid collisions
    let bucket_a0 = format!("disc_{}", Uuid::new_v4());
    let bucket_b1 = format!("disc_{}", Uuid::new_v4());
    let bucket_c0 = format!("disc_{}", Uuid::new_v4());
    let bucket_c1 = format!("disc_{}", Uuid::new_v4());

    create_test_bucket!(
        msg_id,
        owner_a,
        bucket_a0.as_str(),
        0,
        Uuid::new_v4(),
        pool,
        metrics,
        log
    );
    create_test_bucket!(
        msg_id,
        owner_b,
        bucket_b1.as_str(),
        1,
        Uuid::new_v4(),
        pool,
        metrics,
        log
    );
    create_test_bucket!(
        msg_id,
        owner_c,
        bucket_c0.as_str(),
        0,
        Uuid::new_v4(),
        pool,
        metrics,
        log
    );
    create_test_bucket!(
        msg_id,
        owner_c,
        bucket_c1.as_str(),
        1,
        Uuid::new_v4(),
        pool,
        metrics,
        log
    );

    // listowners on vnode 0: should contain owner_a and owner_c
    let owners_v0 = do_listowners(msg_id, 0, &pool, &metrics, &log);
    assert_eq!(
        owners_v0.owners.len(),
        2,
        "expected 2 distinct owners on vnode 0, got {:?}",
        owners_v0.owners
    );
    assert!(owners_v0.owners.contains(&owner_a));
    assert!(owners_v0.owners.contains(&owner_c));

    // listowners on vnode 1: should contain owner_b and owner_c
    let owners_v1 = do_listowners(msg_id, 1, &pool, &metrics, &log);
    assert_eq!(
        owners_v1.owners.len(),
        2,
        "expected 2 distinct owners on vnode 1, got {:?}",
        owners_v1.owners
    );
    assert!(owners_v1.owners.contains(&owner_b));
    assert!(owners_v1.owners.contains(&owner_c));

    // listvnodes should still return the same vnodes (data doesn't
    // change the schema set)
    let vnodes_resp = do_listvnodes(msg_id, &pool, &metrics, &log);
    assert_eq!(vnodes_resp.vnodes, vec![0, 1]);
}

#[test]
fn verify_listowners_invalid_vnode() {
    setup_test_env!(pool, metrics, _log);

    // Use a Critical-level logger to suppress the expected ERRO line
    // from the invalid-vnode error path, keeping test output clean.
    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let log = Logger::root(
        Mutex::new(LevelFilter::new(
            slog_term::FullFormat::new(plain).build(),
            Level::Critical,
        ))
        .fuse(),
        o!(),
    );

    let msg_id: u32 = 0x1;

    // Pick a vnode that definitely does not exist in the test harness
    let bad_vnode: u64 = 99999;

    let payload = discovery::ListOwnersPayload {
        vnode: bad_vnode,
        request_id: Uuid::new_v4(),
    };
    let json = serde_json::to_value(vec![&payload]).unwrap();
    let msg_data = FastMessageData::new("listowners".into(), json);
    let msg = FastMessage::data(msg_id, msg_data);
    let result = util::handle_msg(&msg, &pool, &metrics, &log);
    assert!(result.is_ok());
    let response = result.unwrap();
    assert_eq!(response.len(), 1);

    // The response should be a wrapped PostgresError, not a
    // successful ListOwnersResponse.
    let err_result: Result<BucketsMdapiWrappedError, _> =
        serde_json::from_value(response[0].data.d[0].clone());
    assert!(
        err_result.is_ok(),
        "expected a BucketsMdapiWrappedError for invalid vnode"
    );
    let wrapped = err_result.unwrap();
    assert_eq!(wrapped.error.name, "PostgresError");
    assert!(
        wrapped.error.message.contains("does not exist"),
        "expected 'does not exist' in error message, got: {}",
        wrapped.error.message
    );
}

////////////////////////////////////////////////////////////
// Discovery concurrency and performance tests
////////////////////////////////////////////////////////////

/// Validate that multiple threads can perform discovery
/// RPCs simultaneously without errors or data corruption.
///
/// Spawns 8 threads, each running 20 iterations of
/// listvnodes + listowners on both vnodes, contending on
/// a pool of 5 connections.
#[test]
fn verify_discovery_concurrent_reads() {
    setup_test_env!(pool, metrics, log);
    let msg_id: u32 = 0x1;

    // Seed data: 5 owners on both vnodes (10 buckets).
    let mut expected_v0: HashSet<Uuid> = HashSet::new();
    let mut expected_v1: HashSet<Uuid> = HashSet::new();

    for i in 0u32..5 {
        let owner = Uuid::new_v4();
        let bkt_v0 = format!("conc_r_{}_v0_{}", i, Uuid::new_v4());
        let bkt_v1 = format!("conc_r_{}_v1_{}", i, Uuid::new_v4());

        create_test_bucket!(
            msg_id,
            owner,
            bkt_v0.as_str(),
            0,
            Uuid::new_v4(),
            pool,
            metrics,
            log
        );
        create_test_bucket!(
            msg_id,
            owner,
            bkt_v1.as_str(),
            1,
            Uuid::new_v4(),
            pool,
            metrics,
            log
        );
        expected_v0.insert(owner);
        expected_v1.insert(owner);
    }

    let num_threads = 8;
    let iterations = 20;

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let pool_c = pool.clone();
            let metrics_c = metrics.clone();
            let log_c = log.clone();
            let exp_v0 = expected_v0.clone();
            let exp_v1 = expected_v1.clone();

            thread::spawn(move || {
                for iter in 0..iterations {
                    let mid = ((t * iterations + iter) as u32) + 1;

                    // listvnodes
                    let vr = do_listvnodes(mid, &pool_c, &metrics_c, &log_c);
                    assert!(
                        vr.vnodes.contains(&0) && vr.vnodes.contains(&1),
                        "thread {} iter {}: bad vnodes {:?}",
                        t,
                        iter,
                        vr.vnodes
                    );

                    // listowners vnode 0
                    let o0 = do_listowners(mid, 0, &pool_c, &metrics_c, &log_c);
                    let set0: HashSet<Uuid> = o0.owners.into_iter().collect();
                    assert_eq!(
                        set0, exp_v0,
                        "thread {} iter {}: vnode 0 owners",
                        t, iter
                    );

                    // listowners vnode 1
                    let o1 = do_listowners(mid, 1, &pool_c, &metrics_c, &log_c);
                    let set1: HashSet<Uuid> = o1.owners.into_iter().collect();
                    assert_eq!(
                        set1, exp_v1,
                        "thread {} iter {}: vnode 1 owners",
                        t, iter
                    );
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("reader thread panicked");
    }
}

/// Validate discovery reads return consistent results
/// while concurrent bucket creation is happening.
///
/// Reader threads assert monotonically non-decreasing
/// owner counts (no stale reads showing fewer owners
/// than a previous read).
///
/// Invariant: for each vnode, |owners(t2)| >= |owners(t1)|
///            when t2 > t1 during concurrent inserts.
#[test]
fn verify_discovery_concurrent_reads_writes() {
    setup_test_env!(pool, metrics, log);

    let done = Arc::new(AtomicBool::new(false));
    let num_readers = 4;

    // Spawn reader threads before writes begin.
    let reader_handles: Vec<_> = (0..num_readers)
        .map(|t| {
            let pool_c = pool.clone();
            let metrics_c = metrics.clone();
            let log_c = log.clone();
            let done_c = done.clone();

            thread::spawn(move || {
                let mut prev_v0: usize = 0;
                let mut prev_v1: usize = 0;
                let mut reads: u64 = 0;

                while !done_c.load(Ordering::Relaxed) {
                    let mid =
                        ((t as u32) * 10000) + ((reads as u32) % 10000) + 1;

                    // listvnodes: always [0, 1]
                    let vr = do_listvnodes(mid, &pool_c, &metrics_c, &log_c);
                    assert!(
                        vr.vnodes.contains(&0) && vr.vnodes.contains(&1),
                        "reader {}: bad vnodes {:?}",
                        t,
                        vr.vnodes
                    );

                    // listowners vnode 0: monotonic
                    let o0 = do_listowners(mid, 0, &pool_c, &metrics_c, &log_c);
                    let cur_v0 = o0.owners.len();
                    assert!(
                        cur_v0 >= prev_v0,
                        "reader {}: vnode 0 owners shrank \
                         from {} to {} (read #{})",
                        t,
                        prev_v0,
                        cur_v0,
                        reads
                    );
                    prev_v0 = cur_v0;

                    // listowners vnode 1: monotonic
                    let o1 = do_listowners(mid, 1, &pool_c, &metrics_c, &log_c);
                    let cur_v1 = o1.owners.len();
                    assert!(
                        cur_v1 >= prev_v1,
                        "reader {}: vnode 1 owners shrank \
                         from {} to {} (read #{})",
                        t,
                        prev_v1,
                        cur_v1,
                        reads
                    );
                    prev_v1 = cur_v1;

                    reads += 1;
                }
            })
        })
        .collect();

    // Writer: create 10 buckets with distinct owners,
    // alternating vnodes.
    let msg_id: u32 = 0x1;
    for i in 0u32..10 {
        let owner = Uuid::new_v4();
        let vnode = (i % 2) as u64;
        let bkt = format!("conc_rw_{}_{}", i, Uuid::new_v4());
        create_test_bucket!(
            msg_id,
            owner,
            bkt.as_str(),
            vnode,
            Uuid::new_v4(),
            pool,
            metrics,
            log
        );
    }

    // Signal readers to stop.
    done.store(true, Ordering::Relaxed);

    for h in reader_handles {
        h.join().expect("reader thread panicked");
    }
}

/// Test discovery correctness with a larger, randomized
/// dataset of owners distributed across vnodes.
///
/// Generates 10-20 random owners, each with 1-3 buckets
/// on a random vnode. Validates that listowners returns
/// exactly the expected owner sets and their union covers
/// all owners.
#[test]
fn verify_discovery_random_owners_workload() {
    setup_test_env!(pool, metrics, log);
    let msg_id: u32 = 0x1;

    let mut rng = rand::thread_rng();
    let num_owners: usize = rng.gen_range(10, 21);

    let mut expected_v0: HashSet<Uuid> = HashSet::new();
    let mut expected_v1: HashSet<Uuid> = HashSet::new();
    let mut all_owners: HashSet<Uuid> = HashSet::new();

    for i in 0..num_owners {
        let owner = Uuid::new_v4();
        all_owners.insert(owner);

        let num_buckets: usize = rng.gen_range(1, 4);
        for j in 0..num_buckets {
            let vnode: u64 = rng.gen_range(0, 2);
            let bkt = format!("rand_{}_{}_{}", i, j, Uuid::new_v4());
            create_test_bucket!(
                msg_id,
                owner,
                bkt.as_str(),
                vnode,
                Uuid::new_v4(),
                pool,
                metrics,
                log
            );
            if vnode == 0 {
                expected_v0.insert(owner);
            } else {
                expected_v1.insert(owner);
            }
        }
    }

    // Verify listowners(0)
    let o0 = do_listowners(msg_id, 0, &pool, &metrics, &log);
    let actual_v0: HashSet<Uuid> = o0.owners.into_iter().collect();
    assert_eq!(
        actual_v0,
        expected_v0,
        "vnode 0 owners mismatch: expected {} got {}",
        expected_v0.len(),
        actual_v0.len()
    );

    // Verify listowners(1)
    let o1 = do_listowners(msg_id, 1, &pool, &metrics, &log);
    let actual_v1: HashSet<Uuid> = o1.owners.into_iter().collect();
    assert_eq!(
        actual_v1,
        expected_v1,
        "vnode 1 owners mismatch: expected {} got {}",
        expected_v1.len(),
        actual_v1.len()
    );

    // Union of both vnodes must cover all owners.
    let union: HashSet<Uuid> = actual_v0.union(&actual_v1).cloned().collect();
    assert_eq!(
        union,
        all_owners,
        "union of vnode owners ({}) != all owners ({})",
        union.len(),
        all_owners.len()
    );

    // listvnodes must still report [0, 1]
    let vr = do_listvnodes(msg_id, &pool, &metrics, &log);
    assert!(
        vr.vnodes.contains(&0) && vr.vnodes.contains(&1),
        "expected vnodes [0,1], got {:?}",
        vr.vnodes
    );
}

/// Smoke test that discovery RPCs complete within
/// reasonable time bounds, catching gross performance
/// regressions.
///
/// Uses generous bounds (50ms average per RPC) to avoid
/// flaky CI. Real latency on ephemeral PG is 1-5ms.
#[test]
fn verify_discovery_performance() {
    setup_test_env!(pool, metrics, log);
    let msg_id: u32 = 0x1;

    // Seed 20 owners across both vnodes (40 buckets).
    for i in 0u32..20 {
        let owner = Uuid::new_v4();
        let bkt_v0 = format!("perf_{}_v0_{}", i, Uuid::new_v4());
        let bkt_v1 = format!("perf_{}_v1_{}", i, Uuid::new_v4());
        create_test_bucket!(
            msg_id,
            owner,
            bkt_v0.as_str(),
            0,
            Uuid::new_v4(),
            pool,
            metrics,
            log
        );
        create_test_bucket!(
            msg_id,
            owner,
            bkt_v1.as_str(),
            1,
            Uuid::new_v4(),
            pool,
            metrics,
            log
        );
    }

    // Warm up: prime the connection pool and PG caches.
    let _ = do_listvnodes(msg_id, &pool, &metrics, &log);
    let _ = do_listowners(msg_id, 0, &pool, &metrics, &log);
    let _ = do_listowners(msg_id, 1, &pool, &metrics, &log);

    let iterations: u32 = 100;
    let max_total = std::time::Duration::from_secs(5);

    // Benchmark listvnodes
    let start = Instant::now();
    for i in 0..iterations {
        let vr = do_listvnodes(i + 1, &pool, &metrics, &log);
        assert!(vr.vnodes.contains(&0));
    }
    let elapsed_vnodes = start.elapsed();
    assert!(
        elapsed_vnodes < max_total,
        "listvnodes: {} iterations took {:?} (> {:?})",
        iterations,
        elapsed_vnodes,
        max_total
    );

    // Benchmark listowners(0)
    let start = Instant::now();
    for i in 0..iterations {
        let o0 = do_listowners(i + 1, 0, &pool, &metrics, &log);
        assert_eq!(o0.owners.len(), 20);
    }
    let elapsed_owners_v0 = start.elapsed();
    assert!(
        elapsed_owners_v0 < max_total,
        "listowners(0): {} iterations took {:?} (> {:?})",
        iterations,
        elapsed_owners_v0,
        max_total
    );

    // Benchmark listowners(1)
    let start = Instant::now();
    for i in 0..iterations {
        let o1 = do_listowners(i + 1, 1, &pool, &metrics, &log);
        assert_eq!(o1.owners.len(), 20);
    }
    let elapsed_owners_v1 = start.elapsed();
    assert!(
        elapsed_owners_v1 < max_total,
        "listowners(1): {} iterations took {:?} (> {:?})",
        iterations,
        elapsed_owners_v1,
        max_total
    );
}
