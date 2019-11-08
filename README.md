# boray

A component of the manta buckets storage system for communicating with [Manatee](https://github.com/joyent/manatee).

## Quick start

    cp config.toml.dist config.toml
    vi config.toml
    cargo run -- -c config.toml

## Configuration

Boray features a variety of configuration option to tailor the behavior to
particular environments. The configuration options are organized into categories
and each are described below.

### Logging

The logging configuration entries pertaining the boray's logging behavior.

* `level` - The logging level for boray to use. Possible values are `Critical`,
  `Error`, `Warning`, `Info`, `Debug`, and `Trace`. The default value is `Info`.

### Server

The configuration entries controlling the boray server behavior.

* `host` - The IP address boray should use to listen for incoming connections.
* `port` - The port number boray should listen on for incoming connections.

### Metrics

The configuraiton entries controlling the boray metrics server.

* `host` - The IP address boray should use to listen for metrics requests.
* `port` - The port number boray should listen on for incoming metrics request
  connections.

### Database

The database connection configuration entries.

* `user` - The database username.
* `host` - The database IP address.
* `port` - The database port number.
* `database` - The name of the database to issue requests against.
* `application_name` - The name of the application to use when connecting to the
  database.
* `tls_mode` - The TLS connection mode. Valid values are `disable`, `allow`,
  `prefer`, `require`, `verify-ca`, and `verify-full`. See the [postgres
  client documentation](https://www.postgresql.org/docs/current/libpq-ssl.html) on SSL support for more details about the meaning of
  these options. The default value is `disable`
* `certificate` - The optional path to a TLS certificate file when enabling TLS
  connections via the `tls_mode` configuration option.

### Cueball

The database connection pool configuration entries.

* `max_connections` - The maximum number of connections in the postgres
  connection pool. The default value is 64 connections
* `claim_timeout` - The time in milliseconds that a request to claim a
  connection from the cueball connection pool should wait before returning an
  error. The default is 500 ms.
* `rebalancer_action_delay` - The time in milliseconds to wait prior to
  rebalancing the connection pool when a notification is received from the
  resolver regarding a change in the service topology. For the case of boray
  using the postgres primary resolver this delay should not be very high. The
  default value is 20 ms.

### Tokio

Tokio provides the runtime for boray and these are configuration options to
control the behavior of the Tokio runtime.

* `core_threads` - The maximum number of worker threads for the Tokio Runtime's
  thread pool. This must be a number between 1 and 32,768 though it is advised
  to keep this value on the smaller side. The default value is the number of
  logical cores available to the system.
* `blocking_threads` - The maximum number of concurrent blocking sections in the
  Runtime's thread pool. When the maximum concurrent blocking calls is reached,
  any further calls to blocking will return `NotReady` and the task is notified
  once previously in-flight calls to blocking return. This must be a number
  between 1 and 32,768 though it is advised to keep this value on the smaller
  side. The default value is 200.
* `thread_keep_alive` - The worker thread keep alive duration for threads in the
  Tokio Runtime's thread pool. If set, a worker thread will wait for up to the
  specified duration (in seconds) for work, at which point the thread will
  shutdown. When work becomes available, a new thread will eventually be spawned
  to replace the one that shut down. When the value is `None` (*i.e.* It is
  omitted from the configuration file), the thread will wait for work forever. The default value is `None`.
* `thread_stack_size` - The stack size (in bytes) for worker threads. The
  default is 2 MiB.
* `thread_name_prefix` - The name prefix of threads spawned by the Tokio
  Runtime's thread pool. The default is `boray-woker-`.

## Testing

The tests can be run with:

```
cargo test
```

There are quickcheck tests and also some functional tests for the RPC handlers
that require a functioning postgresql installation as well as
[`pg_tmp`](http://eradman.com/ephemeralpg/). The test uses `pg_tmp` to create
and configure a temporary postgres database and once the test has completed the
temporary database is removed within a few seconds.
