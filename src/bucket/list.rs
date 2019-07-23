/*
 * Copyright 2019 Joyent, Inc.
 */

use std::io::Error as IOError;

use serde_derive::{Deserialize, Serialize};
use serde_json::Value;
use slog::{Logger, debug};
use uuid::Uuid;

use cueball::backend::Backend;
use cueball::connection_pool::ConnectionPool;
use cueball_postgres_connection::PostgresConnection;
use cueball_static_resolver::StaticIpResolver;
use rust_fast::protocol::{FastMessage, FastMessageData};

use tokio_postgres::Error as PGError;
use tokio_postgres::Row as PGRow;

use crate::bucket::BucketResponse;
use crate::sql;
use crate::util::{
    array_wrap,
    other_error
};

#[derive(Serialize, Deserialize)]
pub struct ListBucketsPayload {
    pub owner      : Uuid,
    pub vnode      : u64,
    pub prefix     : Option<String>,
    pub limit      : u64,
    pub marker     : Option<String>,
    pub request_id : Uuid
}

pub fn handler(msg_id: u32,
               args: &[Value],
               mut response: Vec<FastMessage>,
               pool: &ConnectionPool<PostgresConnection, StaticIpResolver, impl FnMut(&Backend) -> PostgresConnection + Send + 'static>,
               log: &Logger) -> Result<Vec<FastMessage>, IOError> {
    debug!(log, "handling listbuckets function request");

    let arg0 = match &args[0] {
        Value::Object(_) => &args[0],
        _ => return Err(other_error("Expected JSON object"))
    };

    let data_clone = arg0.clone();
    let payload_result: Result<ListBucketsPayload, _> =
        serde_json::from_value(data_clone);

    let payload = match payload_result {
        Ok(o) => o,
        Err(_) => return Err(other_error("Failed to parse JSON data as payload for listbuckets function"))
    };

    debug!(log, "parsed ListBucketPayload, req_id: {}", payload.request_id);

    // TODO catch these as errors and return to the caller
    assert!(payload.limit > 0);
    assert!(payload.limit <= 1000);

    // Make db request and form response
    // TODO: make this call safe
    let mut conn = pool.claim().unwrap();
    let mut txn = (*conn).transaction().unwrap();
    let query: Result<Vec<PGRow>, PGError>;

    match (payload.marker, payload.prefix) {
        (Some(marker), Some(prefix)) => {
            let sql = list_sql_prefix_marker(payload.vnode, payload.limit);
            let prefix = format!("{}%", prefix);
            query = sql::txn_query(sql::Method::BucketList, &mut txn, sql.as_str(),
                &[&payload.owner, &prefix, &marker]);
        }
        (Some(marker), None) => {
            let sql = list_sql_marker(payload.vnode, payload.limit);
            query = sql::txn_query(sql::Method::BucketList, &mut txn, sql.as_str(),
                &[&payload.owner, &marker]);
        }
        (None, Some(prefix)) => {
            let sql = list_sql_prefix(payload.vnode, payload.limit);
            let prefix = format!("{}%", prefix);
            query = sql::txn_query(sql::Method::BucketList, &mut txn, sql.as_str(),
                &[&payload.owner, &prefix]);
        }
        (None, None) => {
            let sql = list_sql(payload.vnode, payload.limit);
            query = sql::txn_query(sql::Method::BucketList, &mut txn, sql.as_str(),
                &[&payload.owner]);
        }
    }

    for row in query.unwrap().iter() {
        let resp = BucketResponse {
            id: row.get(0),
            owner: row.get(1),
            name: row.get(2),
            created: row.get(3)
        };

        let value = array_wrap(serde_json::to_value(resp).unwrap());
        let msg = FastMessage::data(msg_id, FastMessageData::new(String::from("listbuckets"), value));
        response.push(msg);
    }

    Ok(response)
}

fn list_sql_prefix_marker(vnode: u64, limit: u64) -> String {
    format!("SELECT id, owner, name, created
        FROM manta_bucket_{}.manta_bucket
        WHERE owner = $1 AND name like $2 AND name > $3
        ORDER BY name ASC
        LIMIT {}",
        vnode, limit)
}

fn list_sql_prefix(vnode: u64, limit: u64) -> String {
    format!("SELECT id, owner, name, created
        FROM manta_bucket_{}.manta_bucket
        WHERE owner = $1 AND name like $2
        ORDER BY name ASC
        LIMIT {}",
        vnode, limit)
}
fn list_sql_marker(vnode: u64, limit: u64) -> String {
    format!("SELECT id, owner, name, created
        FROM manta_bucket_{}.manta_bucket
        WHERE owner = $1 AND name > $2
        ORDER BY name ASC
        LIMIT {}",
        vnode, limit)
}

fn list_sql(vnode: u64, limit: u64) -> String {
    format!("SELECT id, owner, name, created
        FROM manta_bucket_{}.manta_bucket
        WHERE owner = $1
        ORDER BY name ASC
        LIMIT {}",
        vnode, limit)
}
