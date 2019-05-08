/*
 * Copyright 2019 Joyent, Inc.
 */

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};
use std::process::Command;
use std::sync::Mutex;

use slog::{error, info, o, Drain, Logger};
use url::Url;
use uuid::Uuid;

use cueball::connection_pool::ConnectionPool;
use cueball::connection_pool::types::ConnectionPoolOptions;
use cueball_static_resolver::StaticIpResolver;
use cueball_postgres_connection::{PostgresConnection, PostgresConnectionConfig};

use boray::bucket;
use boray::object;

// This test suite requres PostgreSQL and pg_tmp
// (http://eradman.com/ephemeralpg/) to be installed on the test system.
#[test]

fn verify_rpc_handlers() {
    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let log = Logger::root(
        Mutex::new(slog_term::FullFormat::new(plain).build()).fuse(),
        o!("build-id" => "0.1.0"),
    );
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
    // system running the test. The create-ephemeral-db.sh script sets up two
    // vnode schemas (vnodes 0 and 1) for use in testing. This is controlled by
    // the ephermeral-db-schema.sql file in ./tools/postgres.
    ////////////////////////////////////////////////////////////////////////////
    let create_db_output = Command::new("./tools/postgres/create-ephemeral-db.sh")
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
    let user = "postgres";
    let pg_port = pg_url.port().expect("failed to parse postgres port");
    let pg_db = "test";
    let application_name = "boray-test";

    let pg_config = PostgresConnectionConfig {
        user: Some(user.into()),
        password: None,
        host: None,
        port: Some(pg_port),
        database: Some(pg_db.into()),
        application_name: Some(application_name.into())
    };

    let connection_creator = PostgresConnection::connection_creator(pg_config);
    let pool_opts = ConnectionPoolOptions {
        maximum: 5,
        claim_timeout: None,
        log: log.clone(),
        rebalancer_action_delay: None
    };

    let primary_backend = (IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), pg_port);
    let resolver = StaticIpResolver::new(vec![primary_backend]);

    let pool = ConnectionPool::new(
        pool_opts,
        resolver,
        connection_creator
    );

    ////////////////////////////////////////////////////////////////////////////
    // Exercise RPC handlers
    ////////////////////////////////////////////////////////////////////////////
    let msg_id: u32 = 0x1;
    let owner_id = Uuid::new_v4();
    let bucket: String = "testbucket".into();


    // Try to read a bucket
    let get_bucket_payload = bucket::GetBucketPayload {
        owner: owner_id,
        name: bucket.clone(),
        vnode: 0
    };

    let get_bucket_json = serde_json::to_value(get_bucket_payload).unwrap();
    let get_bucket_args = vec![get_bucket_json];
    let mut get_bucket_result =
        bucket::get_handler(msg_id, &get_bucket_args, vec![], &pool, &log);

    assert!(get_bucket_result.is_ok());
    let get_bucket_response = get_bucket_result.unwrap();
    assert_eq!(get_bucket_response.len(), 1);

    let get_bucket_response_result: Result<bucket::BucketNotFoundError, _> =
        serde_json::from_value(get_bucket_response[0].data.d.clone());
    assert!(get_bucket_response_result.is_ok());
    assert_eq!(get_bucket_response_result.unwrap(), bucket::BucketNotFoundError::new());


    // Create a bucket
    let put_bucket_payload = bucket::PutBucketPayload {
        owner: owner_id,
        name: bucket.clone(),
        vnode: 0
    };

    let put_bucket_json = serde_json::to_value(put_bucket_payload).unwrap();
    let put_bucket_args = vec![put_bucket_json];
    let mut put_bucket_result =
        bucket::put_handler(msg_id, &put_bucket_args, vec![], &pool, &log);

    assert!(put_bucket_result.is_ok());
    let put_bucket_response = put_bucket_result.unwrap();
    assert_eq!(put_bucket_response.len(), 1);

    let put_bucket_response_result: Result<bucket::BucketResponse, _> =
        serde_json::from_value(put_bucket_response[0].data.d[0].clone());
    assert!(put_bucket_response_result.is_ok());
    assert_eq!(put_bucket_response_result.unwrap().name, bucket);


    // Read bucket again and make sure the resonse is returned successfully
    get_bucket_result =
        bucket::get_handler(msg_id, &get_bucket_args, vec![], &pool, &log);

    assert!(get_bucket_result.is_ok());
    let get_bucket_response = get_bucket_result.unwrap();
    assert_eq!(get_bucket_response.len(), 1);

    let get_bucket_response_result: Result<bucket::BucketResponse, _> =
        serde_json::from_value(get_bucket_response[0].data.d[0].clone());
    assert!(get_bucket_response_result.is_ok());
    assert_eq!(get_bucket_response_result.unwrap().name, bucket);


    // Try to create same bucket again and verify a BucketAlreadyExists error is
    // returned
    put_bucket_result =
        bucket::put_handler(msg_id, &put_bucket_args, vec![], &pool, &log);

    assert!(put_bucket_result.is_ok());
    let put_bucket_response = put_bucket_result.unwrap();
    assert_eq!(put_bucket_response.len(), 1);

    let put_bucket_response_result: Result<bucket::BucketAlreadyExistsError, _> =
        serde_json::from_value(put_bucket_response[0].data.d.clone());
    assert!(put_bucket_response_result.is_ok());
    assert_eq!(put_bucket_response_result.unwrap(),
               bucket::BucketAlreadyExistsError::new());

    // Delete bucket

    // The get and delete bucket args are the same so we can reuse
    // get_bucket_args here.
    let mut delete_bucket_result =
        bucket::delete_handler(msg_id, &get_bucket_args, vec![], &pool, &log);

    assert!(delete_bucket_result.is_ok());
    let delete_bucket_response = delete_bucket_result.unwrap();
    assert_eq!(delete_bucket_response.len(), 1);

    let delete_bucket_response_result: Result<u64, _> =
        serde_json::from_value(delete_bucket_response[0].data.d[0].clone());
    assert!(delete_bucket_response_result.is_ok());
    assert_eq!(delete_bucket_response_result.unwrap(), 1);

    // Read bucket again and verify it's gone
    get_bucket_result =
        bucket::get_handler(msg_id, &get_bucket_args, vec![], &pool, &log);

    assert!(get_bucket_result.is_ok());
    let get_bucket_response = get_bucket_result.unwrap();
    assert_eq!(get_bucket_response.len(), 1);

    let get_bucket_response_result: Result<bucket::BucketNotFoundError, _> =
        serde_json::from_value(get_bucket_response[0].data.d.clone());
    assert!(get_bucket_response_result.is_ok());
    assert_eq!(get_bucket_response_result.unwrap(), bucket::BucketNotFoundError::new());


    // Attempt to delete a nonexistent bucket and verify an error is returned
    delete_bucket_result =
        bucket::delete_handler(msg_id, &get_bucket_args, vec![], &pool, &log);

    assert!(delete_bucket_result.is_ok());
    let delete_bucket_response = delete_bucket_result.unwrap();
    assert_eq!(delete_bucket_response.len(), 1);

    let delete_bucket_response_result: Result<bucket::BucketNotFoundError, _> =
        serde_json::from_value(delete_bucket_response[0].data.d.clone());
    assert!(delete_bucket_response_result.is_ok());
    assert_eq!(delete_bucket_response_result.unwrap(), bucket::BucketNotFoundError::new());


    // Try to read an object
    let bucket_id = Uuid::new_v4();
    let object: String = "testobject".into();
    let get_object_payload = object::GetObjectPayload {
        owner: owner_id,
        bucket_id,
        name: object.clone(),
        vnode: 1
    };

    let get_object_json = serde_json::to_value(get_object_payload).unwrap();
    let get_object_args = vec![get_object_json];
    let mut get_object_result =
        object::get_handler(msg_id, &get_object_args, vec![], &pool, &log);

    assert!(get_object_result.is_ok());
    let get_object_response = get_object_result.unwrap();
    assert_eq!(get_object_response.len(), 1);

    let get_object_response_result: Result<object::ObjectNotFoundError, _> =
        serde_json::from_value(get_object_response[0].data.d.clone());
    assert!(get_object_response_result.is_ok());
    assert_eq!(get_object_response_result.unwrap(), object::ObjectNotFoundError::new());


    // Create an object
    let put_object_payload = object::PutObjectPayload {
        owner: owner_id,
        bucket_id,
        name: object.clone(),
        vnode: 1,
        content_length: 5,
        content_md5: "xzY5jJbR9rcrMRhlcmi/8g==".into(),
        content_type: "text/plain".into(),
        headers: HashMap::new(),
        sharks: Vec::new(),
        properties: None
    };

    let put_object_json = serde_json::to_value(put_object_payload).unwrap();
    let put_object_args = vec![put_object_json];
    let mut put_object_result =
        object::put_handler(msg_id, &put_object_args, vec![], &pool, &log);

    assert!(put_object_result.is_ok());
    let put_object_response = put_object_result.unwrap();
    assert_eq!(put_object_response.len(), 1);

    let put_object_response_result: Result<object::ObjectResponse, _> =
        serde_json::from_value(put_object_response[0].data.d[0].clone());
    assert!(put_object_response_result.is_ok());
    assert_eq!(put_object_response_result.unwrap().name, object);


    // Read object again and verify a successful response is returned
    get_object_result =
        object::get_handler(msg_id, &get_object_args, vec![], &pool, &log);

    assert!(get_object_result.is_ok());
    let get_object_response = get_object_result.unwrap();
    assert_eq!(get_object_response.len(), 1);

    let get_object_response_result: Result<object::ObjectResponse, _> =
        serde_json::from_value(get_object_response[0].data.d[0].clone());
    assert!(get_object_response_result.is_ok());
    assert_eq!(get_object_response_result.unwrap().name, object);


    // Delete object

    // The get and delete object args are the same so we can reuse
    // get_object_args here.
    let mut delete_object_result =
        object::delete_handler(msg_id, &get_object_args, vec![], &pool, &log);

    assert!(delete_object_result.is_ok());
    let delete_object_response = delete_object_result.unwrap();
    assert_eq!(delete_object_response.len(), 1);

    let delete_object_response_result: Result<u64, _> =
        serde_json::from_value(delete_object_response[0].data.d[0].clone());
    assert!(delete_object_response_result.is_ok());
    assert_eq!(delete_object_response_result.unwrap(), 1);


    // Read object again and verify it is not found
    get_object_result =
        object::get_handler(msg_id, &get_object_args, vec![], &pool, &log);

    assert!(get_object_result.is_ok());
    let get_object_response = get_object_result.unwrap();
    assert_eq!(get_object_response.len(), 1);

    let get_object_response_result: Result<object::ObjectNotFoundError, _> =
        serde_json::from_value(get_object_response[0].data.d.clone());
    assert!(get_object_response_result.is_ok());
    assert_eq!(get_object_response_result.unwrap(), object::ObjectNotFoundError::new());

    // Delete the object again and verify it is not found
    delete_object_result =
        object::delete_handler(msg_id, &get_object_args, vec![], &pool, &log);

    assert!(delete_object_result.is_ok());
    let delete_object_response = delete_object_result.unwrap();
    assert_eq!(delete_object_response.len(), 1);

    let delete_object_response_result: Result<object::ObjectNotFoundError, _> =
        serde_json::from_value(delete_object_response[0].data.d.clone());
    assert!(delete_object_response_result.is_ok());
    assert_eq!(delete_object_response_result.unwrap(), object::ObjectNotFoundError::new());

    // List buckets and confirm none are found

    let list_buckets_payload = bucket::ListBucketsPayload {
        owner: owner_id,
        vnode: 0,
        prefix: "testbucket".into(),
        order_by: "created".into(),
        limit: 1000,
        offset: 0
    };

    let list_buckets_json = serde_json::to_value(list_buckets_payload).unwrap();
    let list_buckets_args = vec![list_buckets_json];
    let mut list_buckets_result =
        bucket::list_handler(msg_id, &list_buckets_args, vec![], &pool, &log);

    assert!(list_buckets_result.is_ok());
    let list_buckets_response = list_buckets_result.unwrap();
    assert_eq!(list_buckets_response.len(), 0);

    // Create a bucket and list buckets again
    put_bucket_result =
        bucket::put_handler(msg_id, &put_bucket_args, vec![], &pool, &log);

    assert!(put_bucket_result.is_ok());
    let put_bucket_response = put_bucket_result.unwrap();
    assert_eq!(put_bucket_response.len(), 1);

    let put_bucket_response_result: Result<bucket::BucketResponse, _> =
        serde_json::from_value(put_bucket_response[0].data.d[0].clone());
    assert!(put_bucket_response_result.is_ok());
    assert_eq!(put_bucket_response_result.unwrap().name, bucket);

    list_buckets_result =
        bucket::list_handler(msg_id, &list_buckets_args, vec![], &pool, &log);

    assert!(list_buckets_result.is_ok());
    let list_buckets_response = list_buckets_result.unwrap();
    assert_eq!(list_buckets_response.len(), 1);


    // List objects and confirm none are found

    let list_objects_payload = object::ListObjectsPayload {
        owner: owner_id,
        bucket_id,
        vnode: 1,
        prefix: "testobject".into(),
        order_by: "created".into(),
        limit: 1000,
        offset: 0
    };

    let list_objects_json = serde_json::to_value(list_objects_payload).unwrap();
    let list_objects_args = vec![list_objects_json];
    let mut list_objects_result =
        object::list_handler(msg_id, &list_objects_args, vec![], &pool, &log);

    assert!(list_objects_result.is_ok());
    let list_objects_response = list_objects_result.unwrap();
    assert_eq!(list_objects_response.len(), 0);

    // Create an object and list objects again
    put_object_result =
        object::put_handler(msg_id, &put_object_args, vec![], &pool, &log);

    assert!(put_object_result.is_ok());
    let put_object_response = put_object_result.unwrap();
    assert_eq!(put_object_response.len(), 1);

    let put_object_response_result: Result<object::ObjectResponse, _> =
        serde_json::from_value(put_object_response[0].data.d[0].clone());
    assert!(put_object_response_result.is_ok());
    assert_eq!(put_object_response_result.unwrap().name, object);

    list_objects_result =
        object::list_handler(msg_id, &list_objects_args, vec![], &pool, &log);

    assert!(list_objects_result.is_ok());
    let list_objects_response = list_objects_result.unwrap();
    assert_eq!(list_objects_response.len(), 1);

}
