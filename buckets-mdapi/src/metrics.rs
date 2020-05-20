// Copyright 2020 Joyent, Inc.

use std::collections::HashMap;
use std::net::SocketAddr;

use gethostname::gethostname;
use hyper::header::{HeaderValue, CONTENT_TYPE};
use hyper::rt::{self, Future};
use hyper::server::Server;
use hyper::service::service_fn_ok;
use hyper::Body;
use hyper::StatusCode;
use hyper::{Request, Response};
use prometheus::{
    labels, opts, register_counter, Counter, Encoder, HistogramOpts,
    HistogramVec, TextEncoder,
};

use slog::{error, info, Logger};

use utils::config::ConfigMetrics;

// 1.0 == 1 second
const HISTOGRAM_BUCKETS: [f64; 28] = [
    0.0001, 0.0002, 0.0003, 0.0004, 0.0005, 0.0006, 0.0007, 0.0008, 0.0009,
    0.001, 0.002, 0.003, 0.004, 0.005, 0.006, 0.007, 0.008, 0.009, 0.01, 0.025,
    0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

#[derive(Clone)]
pub struct RegisteredMetrics {
    pub request_count: Counter,
    pub metrics_request_count: Counter,
    pub fast_requests: HistogramVec,
    pub postgres_requests: HistogramVec,
    pub connection_claim_times: HistogramVec,
}

impl RegisteredMetrics {
    fn new(
        request_count: Counter,
        metrics_request_count: Counter,
        fast_requests: HistogramVec,
        postgres_requests: HistogramVec,
        connection_claim_times: HistogramVec,
    ) -> Self {
        RegisteredMetrics {
            request_count,
            metrics_request_count,
            fast_requests,
            postgres_requests,
            connection_claim_times,
        }
    }
}

pub fn register_metrics(config: &ConfigMetrics) -> RegisteredMetrics {
    let hostname = gethostname()
        .into_string()
        .unwrap_or_else(|_| String::from("unknown"));
    let request_counter = register_counter!(opts!(
        "incoming_request_count",
        "Total number of Fast requests handled.",
        labels! {"datacenter" => config.datacenter.as_str(),
                 "service" => config.service.as_str(),
                 "server" => config.server.as_str(),
                 "zonename" => hostname.as_str(),
        }
    ))
    .expect("failed to register incoming_request_count counter");

    let metrics_request_counter = register_counter!(opts!(
        "metrics_request_count",
        "Total number of metrics requests received.",
        labels! {"datacenter" => config.datacenter.as_str(),
                 "service" => config.service.as_str(),
                 "server" => config.server.as_str(),
                 "zonename" => hostname.as_str(),
        }
    ))
    .expect("failed to register metrics_request_count counter");

    let mut const_labels = HashMap::new();
    const_labels.insert("service".to_string(), config.service.clone());
    const_labels.insert("server".to_string(), config.server.clone());
    const_labels.insert("datacenter".to_string(), config.datacenter.clone());
    const_labels.insert("zonename".to_string(), hostname);

    let fast_requests = register_histogram(
        "fast_requests",
        "Latency of all fast requests processed.",
        &const_labels,
        vec!["method", "success"],
    );

    let postgres_requests = register_histogram(
        "postgres_requests",
        "Latency of all postgres requests processed.",
        &const_labels,
        vec!["method", "success"],
    );

    let connection_claim_times = register_histogram(
        "connection_claim_times",
        "Wait time to acquire a postgres connection from the connection pool.",
        &const_labels,
        vec!["success"],
    );

    RegisteredMetrics::new(
        request_counter,
        metrics_request_counter,
        fast_requests,
        postgres_requests,
        connection_claim_times,
    )
}

fn register_histogram(
    name: &str,
    description: &str,
    const_labels: &HashMap<String, String>,
    labels: Vec<&str>,
) -> HistogramVec {
    let opts = HistogramOpts::new(name, description)
        .const_labels(const_labels.clone())
        .buckets(HISTOGRAM_BUCKETS.to_vec());
    let h_vec =
        HistogramVec::new(opts, labels.as_slice()).unwrap_or_else(|_| {
            panic!(["failed to create ", name, " histogram"].concat())
        });

    prometheus::register(Box::new(h_vec.clone())).unwrap_or_else(|_| {
        panic!(["failed to register ", name, " histogram"].concat())
    });

    h_vec
}

pub fn start_server(
    address: &str,
    port: u16,
    metrics: RegisteredMetrics,
    log: &Logger,
) {
    let addr = [&address, ":", &port.to_string()]
        .concat()
        .parse::<SocketAddr>()
        .unwrap();

    let log_clone = log.clone();

    let server = Server::bind(&addr)
        .serve(move || {
            let metrics_request_count = metrics.metrics_request_count.clone();
            service_fn_ok(move |_: Request<Body>| {
                metrics_request_count.inc();

                let metric_families = prometheus::gather();
                let mut buffer = vec![];
                let encoder = TextEncoder::new();
                encoder.encode(&metric_families, &mut buffer).unwrap();

                let content_type =
                    encoder.format_type().parse::<HeaderValue>().unwrap();

                Response::builder()
                    .header(CONTENT_TYPE, content_type)
                    .status(StatusCode::OK)
                    .body(Body::from(buffer))
                    .unwrap()
            })
        })
        .map_err(
            move |e| error!(log_clone, "metrics server error"; "error" => %e),
        );

    info!(log, "listening"; "address" => addr);

    rt::run(server);
}
