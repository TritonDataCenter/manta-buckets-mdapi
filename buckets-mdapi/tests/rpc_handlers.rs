// Copyright 2020 Joyent, Inc.
// Copyright 2023 MNX Cloud, Inc.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};
use std::path::Path;
use std::process::Command;
use std::sync::Mutex;

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
use buckets_mdapi::error::{BucketsMdapiError, BucketsMdapiWrappedError};
use buckets_mdapi::gc;
use buckets_mdapi::metrics;
use buckets_mdapi::object;
use buckets_mdapi::util;
use utils::{config, schema};

// This test suite requires PostgreSQL and pg_tmp
// (http://eradman.com/ephemeralpg/) to be installed on the test system.
#[test]

fn verify_rpc_handlers() {
    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let log = Logger::root(
        Mutex::new(LevelFilter::new(
            slog_term::FullFormat::new(plain).build(),
            Level::Error,
        ))
        .fuse(),
        o!(),
    );

    let metrics_config = config::ConfigMetrics::default();
    let metrics = metrics::register_metrics(&metrics_config);

    ////////////////////////////////////////////////////////////////////////////
    // Check for pg_tmp on the system
    ////////////////////////////////////////////////////////////////////////////
    let pg_tmp_check_output = Command::new("which")
        .arg("pg_tmp")
        .output()
        .expect("failed to execute process");

    if !pg_tmp_check_output.status.success() {
        error!(log, "pg_tmp is required to run this test");
    }
    assert!(pg_tmp_check_output.status.success());

    ////////////////////////////////////////////////////////////////////////////
    // Create pg_tmp database. This requires that pg_tmp be installed on the
    // system running the test.
    ////////////////////////////////////////////////////////////////////////////
    let create_db_output =
        Command::new("../tools/postgres/create-ephemeral-db.sh")
            .output()
            .expect("failed to execute process");

    assert!(create_db_output.status.success());

    let pg_connect_str = String::from_utf8_lossy(&create_db_output.stdout);

    info!(log, "pg url: {}", pg_connect_str);

    let pg_url = Url::parse(&pg_connect_str)
        .expect("failed to parse postgres connection string");

    ////////////////////////////////////////////////////////////////////////////
    // Create connection pool
    ////////////////////////////////////////////////////////////////////////////
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

    let connection_creator = PostgresConnection::connection_creator(pg_config);
    let pool_opts = ConnectionPoolOptions {
        max_connections: Some(5),
        claim_timeout: None,
        log: Some(log.clone()),
        rebalancer_action_delay: None,
        decoherence_interval: None,
        connection_check_interval: None,
    };

    let primary_backend = (IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), pg_port);
    let resolver = StaticIpResolver::new(vec![primary_backend]);

    let pool = ConnectionPool::new(pool_opts, resolver, connection_creator);

    ////////////////////////////////////////////////////////////////////////////
    // Setup the vnode schemas
    //
    // Use the schema-manager functions to create the vnode schemas in the
    // postgres database. This sets up two vnode schemas (vnodes 0 and 1).
    ////////////////////////////////////////////////////////////////////////////

    let template_dir = "../schema_templates";
    let migrations_dir = Path::new("../migrations");

    let mut conn = pool
        .claim()
        .expect("failed to acquire postgres connection for vnode schema setup");

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
        &log,
    )
    .expect("failed to create vnode schemas");

    drop(conn);

    ////////////////////////////////////////////////////////////////////////////
    // Exercise RPC handlers
    ////////////////////////////////////////////////////////////////////////////
    let msg_id: u32 = 0x1;
    let owner_id = Uuid::new_v4();
    let bucket: String = "testbucket".into();
    let request_id = Uuid::new_v4();

    // Try to read a bucket
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
     * All errors should have a "name" and "message" property to comply with the error format in
     * the Fast protocol.
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

    // Read bucket again and make sure the resonse is returned successfully
    get_bucket_result =
        util::handle_msg(&get_bucket_fast_msg, &pool, &metrics, &log);

    assert!(get_bucket_result.is_ok());
    let get_bucket_response = get_bucket_result.unwrap();
    assert_eq!(get_bucket_response.len(), 1);

    let get_bucket_response_result: Result<bucket::BucketResponse, _> =
        serde_json::from_value(get_bucket_response[0].data.d[0].clone());
    assert!(get_bucket_response_result.is_ok());
    assert_eq!(get_bucket_response_result.unwrap().name, bucket);

    // Try to create same bucket again and verify a BucketAlreadyExists error is
    // returned
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

    // Read bucket again and verify it's gone
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

    // Attempt to delete a nonexistent bucket and verify an error is returned
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

    // Try to read an object
    let bucket_id = Uuid::new_v4();
    let object: String = "testobject".into();
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

    // Try to update an nonexistent object's metadata
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

    let update_object_payload = object::update::UpdateObjectPayload {
        owner: owner_id,
        bucket_id,
        name: object.clone(),
        id: object_id,
        vnode: 1,
        content_type: "text/html".into(),
        headers: update_headers,
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

    // Create an object, fail with bad etag
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

    // Create an object, ensure nothing was there before us
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

    // Create an object, fail with bad etag
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

    // Read object again and verify a successful response is returned
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

    // Update the object's metadata and verify it is successful
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

    // Read object again and verify the metadata update
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

    // Get object with "if-match: correctETag"
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

    // Try get object with "if-match: wrongETag"
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

    // The get and delete object args are the same so we can reuse
    // get_object_json here.  Just lets empty the conditions first.
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

    // Read object again and verify it is not found
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

    // Delete the object again and verify it is not found
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

    // List buckets and confirm none are found

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

    // Create a bucket and list buckets again
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

    // List objects and confirm none are found

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

    // Create an object and list objects again
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

    // Exercise the garbage collection functions

    // First request a batch of garbage
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

    // Request the batch again and verify that the reported batch id matches the
    // one from the previous request.

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

    // Indicate that the batch of garbage is processed and request for it to be
    // deleted, but use a batch_id that does not match the id of the current
    // batch.
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

    // Now indicate that the batch of garbage is processed and request for it to
    // be deleted. This also verifies the logic that ensures the previous
    // request to delete the gc batch using an invalid batch id does not
    // actually result in the garbage batch being removed.
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
    // let mut delete_garbage_responses = delete_garbage_result.unwrap();
    delete_garbage_responses = delete_garbage_result.unwrap();
    assert_eq!(delete_garbage_responses.len(), 1);

    // let mut delete_garbage_response_result: Result<String, _> =
    //     serde_json::from_value(delete_garbage_response[0].data.d[0].clone());
    delete_garbage_response_result =
        serde_json::from_value(delete_garbage_responses[0].data.d[0].clone());
    assert!(delete_garbage_response_result.is_ok());
    delete_garbage_response = delete_garbage_response_result.unwrap();
    assert_eq!(&delete_garbage_response, "ok");

    // Request another batch of garbage and this time it should return an empty
    // list and a NULL batch_id
    get_garbage_result =
        util::handle_msg(&get_garbage_fast_msg, &pool, &metrics, &log);

    assert!(get_garbage_result.is_ok());
    let get_garbage_response = get_garbage_result.unwrap();
    assert_eq!(get_garbage_response.len(), 1);

    let get_garbage_response_result: Result<gc::get::GetGarbageResponse, _> =
        serde_json::from_value(get_garbage_response[0].data.d[0].clone());

    println!("ggr: {:?}", get_garbage_response);
    assert!(get_garbage_response_result.is_ok());
    get_garbage_unwrapped_result = get_garbage_response_result.unwrap();
    assert!(get_garbage_unwrapped_result.batch_id.is_none());
    assert!(get_garbage_unwrapped_result.garbage.is_empty());
}
