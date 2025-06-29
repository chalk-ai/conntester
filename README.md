# PostgreSQL Connection Tester

A simple Go program that tests PostgreSQL connection acquisition time and reports metrics to Datadog.

## Features

- Measures PostgreSQL connection acquisition time
- Emits Datadog StatsD metrics for connection latency and attempt counts
- Configurable connection timeout
- Cross-compilation for x86_64 Linux (Debian containers)

## Metrics

The program emits the following metrics:

- `chalk.conntester.connection_acquisition_duration` - Distribution metric of connection time
- `chalk.conntester.attempt_count` - Count metric for connection attempts

Both metrics are tagged with `status:success` or `status:failure`.

## Usage

```
./conntester -uri "postgres://username:password@localhost:5432/dbname?sslmode=disable" [-timeout 5] [-statsd "127.0.0.1:8125"]
```

### Parameters

- `-uri` (required): PostgreSQL connection URI
- `-timeout` (optional): Connection timeout in seconds (default: 5)
- `-statsd` (optional): StatsD server address (default: "127.0.0.1:8125")

## Building

To build for the local platform:

```
go build -o conntester ./cmd/conntester
```

To build for x86_64 Linux (Debian containers):

```
./build.sh
```

This will create a binary at `build/conntester` that can be deployed to a Debian Linux container.