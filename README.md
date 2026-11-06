# Phorcys

![Phorcys for RabbitMQ](logo.jpg)

*Pronunciation: **FAWR**-sis (`/ˈfɔːrsɪs/`)*

> In Greek mythology, Phorcys represents the hidden dangers of the
> deep sea.

Phorcys is a self-healing recovery service for RabbitMQ **quorum queues**. It
monitors the broker's internal log exchange for Raft/quorum error signatures,
evaluates whether a flagged queue is truly unrecoverable, and — when it is —
automatically backs up the on-disk data, deletes the corrupted queue, carves
recoverable messages from the backup files, and republishes them to the
original queue.

## How it works

```text
amq.rabbitmq.log (warning/error)
        │
        ▼
  Log Monitor ──► pre-filter (ra/quorum/raft keywords)
        │
        ▼
  Queue name match against tracked quorum queues
        │
        ▼
  Health Evaluator
    ├─ GREEN          → no action
    ├─ TRANSIENT_DOWN → no action (resource alarm, node restart, cluster-wide outage)
    └─ UNRECOVERABLE  → Recovery Pipeline
              │
              ├─ 1. Locate Raft data directory on disk
              ├─ 2. Backup .segment / .wal files
              ├─ 3. Delete corrupted queue from broker
              ├─ 4. Carve AMQP payloads from backup (ETF byte-scan)
              └─ 5. Republish to default exchange → original queue
```

### Health classification

| State            | Condition                                                                                    | Action                |
|------------------|----------------------------------------------------------------------------------------------|-----------------------|
| `GREEN`          | Queue has a leader and is running                                                            | None                  |
| `TRANSIENT_DOWN` | Memory/disk alarm, host node down, or all neighbors also down                                | Wait — do not delete  |
| `UNRECOVERABLE`  | Management API returns 500 for this queue, **or** no Raft leader while neighbors are healthy | Run recovery pipeline |

## Configuration

All settings are provided via environment variables with sensible defaults for
local development.

| Variable                    | Default                                                             | Description                                                                                                                 |
|-----------------------------|---------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| `PHORCYS_AMQP_URL`          | `amqp://guest:guest@localhost:5672/`                                | AMQP broker URL                                                                                                             |
| `PHORCYS_MGMT_URL`          | `http://localhost:15672`                                            | Management API base URL                                                                                                     |
| `PHORCYS_MGMT_USER`         | `guest`                                                             | Management API username                                                                                                     |
| `PHORCYS_MGMT_PASS`         | `guest`                                                             | Management API password                                                                                                     |
| `PHORCYS_QUORUM_PATH`       | `/var/lib/rabbitmq/mnesia/rabbit@localhost/quorum/rabbit@localhost` | Base path of the quorum queue Raft storage on disk                                                                          |
| `PHORCYS_BACKUP_DIR`        | `/var/lib/rabbitmq/phorcys-backups`                                 | Directory where queue data is backed up before deletion                                                                     |
| `PHORCYS_CONSENSUS_TIMEOUT` | `120s`                                                              | A Go duration (e.g. "90s") bounding how long a node waits for other replicas' announcements before choosing a recovery node |

### TLS

Use `amqps://` in `PHORCYS_AMQP_URL` and `https://` in `PHORCYS_MGMT_URL` for
a TLS-enabled broker.
If the broker certificate is signed by a CA that is not in the system trust
store, or the broker requires client certificates, set:

| Variable                           | Description                                                                           |
|------------------------------------|---------------------------------------------------------------------------------------|
| `PHORCYS_TLS_CA_FILE`              | PEM file with the CA certificate(s) to trust, in addition to the system trust store   |
| `PHORCYS_TLS_CERT_FILE`            | PEM client certificate for mutual TLS (requires `PHORCYS_TLS_KEY_FILE`)               |
| `PHORCYS_TLS_KEY_FILE`             | PEM private key for the client certificate                                            |
| `PHORCYS_TLS_SERVER_NAME`          | Hostname to verify the server certificate against, if different from the URL host     |
| `PHORCYS_TLS_INSECURE_SKIP_VERIFY` | `true` disables certificate verification (development only)                           |

The same settings apply to both the AMQP and Management API connections.

## Building

```bash
go build -o phorcys .
```

## Running

Ensure the following options are configured within RabbitMQ:

```bash
log.exchange = true
log.exchange.level = warning
```

```bash
export PHORCYS_AMQP_URL="amqp://admin:secret@rabbitmq:5672/" # betterleaks:allow
export PHORCYS_MGMT_URL="http://rabbitmq:15672"
export PHORCYS_MGMT_USER="admin"
export PHORCYS_MGMT_PASS="secret"
export PHORCYS_QUORUM_PATH="/var/lib/rabbitmq/mnesia/rabbit@rabbitmq/quorum/rabbit@rabbitmq"
export PHORCYS_BACKUP_DIR="/data/phorcys-backups"

./phorcys
```

Phorcys runs until it receives `SIGINT` or `SIGTERM`.

### Manual recovery

If a quorum queue is corrupted but was not detected by the log monitor, the
recovery pipeline can be run on demand. It uses the same environment variables
as the monitor:

```bash
./phorcys recover -queue orders -vhost /
```

| Flag     | Default | Description                                    |
|----------|---------|------------------------------------------------|
| `-queue` | —       | Name of the quorum queue to recover (required) |
| `-vhost` | `/`     | Virtual host of the queue                      |
| `-yes`   | `false` | Skip the interactive confirmation prompt       |

The queue is **deleted** from the broker before its recovered messages are
republished, so you are asked to confirm unless `-yes` is passed.

## Requirements

- RabbitMQ 3.8+ with quorum queues enabled
- The process must run on the **same host** as the RabbitMQ node (for
  filesystem access to Raft data and `rabbitmqctl` fallback eviction)
- The user configured by PHORCYS_MGMT_USER and the user embedded in
  PHORCYS_AMQP_URL must have full(configure, read, write) access
  to all Virtual Host within RabbitMQ.
- The RabbitMQ System log exchange must be enabled.
- Go 1.21+ to build from source

## Project layout

```text
phorcys/
├── main.go                 # Entry point
├── internal/
│   ├── cmd/
│   │   └── recover/        # `phorcys recover` manual recovery subcommand
│   ├── amqpx/
│   │   ├── amqpx.go        # AMQP Connection/Channel interfaces
│   │   ├── shim.go         # Adapter over amqp091-go (live broker)
│   │   └── amqptest/       # In-memory AMQP fakes for unit tests
│   ├── runtime/
│   │   └── config.go       # Config loading
│   ├── monitor/
│   │   └── monitor.go      # Subscribes to amq.rabbitmq.log
│   ├── broker/
│   │   ├── diagnostics.go  # Management API client
│   │   ├── delete.go       # Queue deletion + rabbitmqctl fallback
│   │   └── types.go        # API models, EvaluateQueueHealth
│   ├── recovery/
│   │   ├── pipeline.go     # Orchestrates the recovery steps
│   │   ├── locate.go       # Find queue's Raft data dir via ETF
│   │   ├── backup.go       # Copy WAL files for a queue's UID
│   │   ├── carve.go        # Extract AMQP payloads from files
│   │   ├── etfscan.go      # Low-level ETF tag scanning
│   │   ├── wal.go          # Ra WAL file record parsing
│   │   ├── copyfile.go     # File copy helper
│   │   ├── republish.go    # Republish payloads via AMQP
│   │   └── types.go        # Shared recovery data types
│   └── testutil/
│       └── testutil.go     # Shared test helpers
└── tests/
    └── integration/        # Docker-based e2e tests
```

## Integration tests

The three-node cluster in `tests/integration` runs with plain AMQP by default,
or TLS-only listeners when the TLS overlay is added:

```bash
cd tests/integration

# plain
docker compose up --build -d
go test -tags integration -count=1 ./...

# TLS
./gen-certs.sh
docker compose -f docker-compose.yml -f docker-compose.tls.yml up --build -d
export RABBITMQ_URL=amqps://guest:guest@localhost:5671/ # betterleaks:allow
RABBITMQ_TLS_CA_FILE=$PWD/certs/ca.pem \
  go test -tags integration -count=1 ./...
```

## Limitations & caveats

- **Best-effort recovery.** The ETF carver scans raw bytes heuristically.
  Messages that span corrupted sectors may not be recoverable.
- **Single-node access required.** Phorcys must run on the RabbitMQ node that
  hosts the quorum queue data. In a multi-node cluster, run one Phorcys instance
  per node.
- **AMQP message metadata is not preserved.** Recovered messages are
  republished as `application/octet-stream` with `DeliveryMode=Persistent`.
  Original headers, content-type, and routing metadata are not reconstructed.
- **Duplicate delivery is possible.** If a message was already acknowledged
  before the crash, carving may recover and republish it again.

## Contributing

Maintainers are accepting patches and contributions to this project.
Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details about submitting
contributions to this project.

## Security policy

Please see our [Security Policy](SECURITY.md) for details.

## License

This project is licensed under the [Apache 2.0 License](LICENSE).

## Third-party dependencies

<!-- markdownlint-disable MD013 -->
| Dependency                       | License                                                             |
|----------------------------------|---------------------------------------------------------------------|
| `github.com/DeedleFake/etf`      | [LICENSE](https://github.com/DeedleFake/etf/blob/master/LICENSE)    |
| `github.com/rabbitmq/amqp091-go` | [LICENSE](https://github.com/rabbitmq/amqp091-go/blob/main/LICENSE) |
| `github.com/stretchr/testify`    | [LICENSE](https://github.com/stretchr/testify/blob/master/LICENSE)  |
<!-- markdownlint-enable MD013 -->
