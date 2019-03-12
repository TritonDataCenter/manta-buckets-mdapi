/*
 * Copyright 2019 Joyent, Inc.
 */

use std::net::SocketAddr;

use hyper::Body;
use hyper::header::{CONTENT_TYPE, HeaderValue};
use hyper::{Request, Response};
use hyper::rt::{self, Future};
use hyper::server::Server;
use hyper::service::service_fn_ok;
use hyper::StatusCode;
use prometheus::{Counter, Encoder, TextEncoder};
use slog::{Logger, info};


lazy_static! {
    pub static ref INCOMING_REQUEST_COUNTER: Counter = register_counter!(opts!(
        "incoming_request_count",
        "Total number of Fast requests handled.",
        labels! {"handler" => "all",}
    )).unwrap();
    pub static ref METRICS_REQUEST_COUNTER: Counter = register_counter!(opts!(
        "metrics_request_count",
        "Total number of metrics requests received.",
        labels! {"handler" => "all",}
    )).unwrap();
}

pub fn start_server(address: String, port: u32, log: Logger) {
    let addr = [address, String::from(":"), port.to_string()]
        .concat()
        .parse::<SocketAddr>()
        .unwrap();

    let log_clone = log.clone();

    let server = Server::bind(&addr)
        .serve(|| {
            service_fn_ok(move |_: Request<Body>| {
                METRICS_REQUEST_COUNTER.inc();

                let metric_families = prometheus::gather();
                let mut buffer = vec![];
                let encoder = TextEncoder::new();
                encoder.encode(&metric_families, &mut buffer).unwrap();

                let content_type = encoder.format_type()
                    .parse::<HeaderValue>()
                    .unwrap();

                Response::builder()
                    .header(CONTENT_TYPE, content_type)
                    .status(StatusCode::OK)
                    .body(Body::from(buffer))
                    .unwrap()
            })
        })
        .map_err(move |e| error!(log_clone, "metrics server error: {}", e));

    info!(log, "metrics server listening at {:?}", addr);

    rt::run(server);
}
