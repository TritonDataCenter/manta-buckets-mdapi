// Copyright 2019 Joyent, Inc.

use std::net::SocketAddr;

use hyper::header::{HeaderValue, CONTENT_TYPE};
use hyper::rt::{self, Future};
use hyper::server::Server;
use hyper::service::service_fn_ok;
use hyper::Body;
use hyper::StatusCode;
use hyper::{Request, Response};
use lazy_static::lazy_static;
use prometheus::{
    histogram_opts, labels, opts, register_counter, register_histogram_vec, Counter, Encoder,
    HistogramVec, TextEncoder,
};
use slog::{error, info, Logger};

lazy_static! {
    pub static ref INCOMING_REQUEST_COUNTER: Counter = register_counter!(opts!(
        "incoming_request_count",
        "Total number of Fast requests handled.",
        labels! {"handler" => "all",}
    ))
    .unwrap();
    pub static ref METRICS_REQUEST_COUNTER: Counter = register_counter!(opts!(
        "metrics_request_count",
        "Total number of metrics requests received.",
        labels! {"handler" => "all",}
    ))
    .unwrap();
    pub static ref FAST_REQUESTS: HistogramVec = register_histogram_vec!(
        "fast_requests",
        "Latency of all fast requests processed.",
        &["method", "success"]
    )
    .unwrap();
    pub static ref POSTGRES_REQUESTS: HistogramVec = register_histogram_vec!(
        "postgres_requests",
        "Latency of all postgres requests processed.",
        &["method", "success"]
    )
    .unwrap();
}

pub fn start_server(address: &str, port: u16, log: &Logger) {
    let addr = [&address, ":", &port.to_string()]
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

                let content_type = encoder.format_type().parse::<HeaderValue>().unwrap();

                Response::builder()
                    .header(CONTENT_TYPE, content_type)
                    .status(StatusCode::OK)
                    .body(Body::from(buffer))
                    .unwrap()
            })
        })
        .map_err(move |e| error!(log_clone, "metrics server error"; "error" => %e));

    info!(log, "listening"; "address" => addr);

    rt::run(server);
}
