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

#[derive(Clone)]
pub struct RegisteredMetrics {
    pub request_count: Counter,
    pub metrics_request_count: Counter,
    pub fast_requests: HistogramVec,
    pub postgres_requests: HistogramVec,
}

impl RegisteredMetrics {
    fn new(
        request_count: Counter,
        metrics_request_count: Counter,
        fast_requests: HistogramVec,
        postgres_requests: HistogramVec,
    ) -> Self {
        RegisteredMetrics {
            request_count,
            metrics_request_count,
            fast_requests,
            postgres_requests,
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

    let fast_requests_opts = HistogramOpts::new(
        "fast_requests",
        "Latency of all fast requests processed.",
    )
    .const_labels(const_labels.clone());
    let fast_requests =
        HistogramVec::new(fast_requests_opts, &["method", "success"])
            .expect("failed to create fast_requests histogram");

    prometheus::register(Box::new(fast_requests.clone()))
        .expect("failed to register fast_requests histogram");

    let postgres_requests_opts = HistogramOpts::new(
        "postgres_requests",
        "Latency of all fast requests processed.",
    )
    .const_labels(const_labels);
    let postgres_requests =
        HistogramVec::new(postgres_requests_opts, &["method", "success"])
            .expect("failed to create postgres_requests histogram");

    prometheus::register(Box::new(postgres_requests.clone()))
        .expect("failed to register postgres_requests histogram");

    RegisteredMetrics::new(
        request_counter,
        metrics_request_counter,
        fast_requests,
        postgres_requests,
    )
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
