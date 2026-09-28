# Phorcys

![Phorcys for RabbitMQ](logo.jpg)

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

| Variable              | Default                                                             | Description                                             |
|-----------------------|---------------------------------------------------------------------|---------------------------------------------------------|
| `PHORCYS_AMQP_URL`    | `amqp://guest:guest@localhost:5672/`                                | AMQP broker URL                                         |
| `PHORCYS_MGMT_URL`    | `http://localhost:15672`                                            | Management API base URL                                 |
| `PHORCYS_MGMT_USER`   | `guest`                                                             | Management API username                                 |
| `PHORCYS_MGMT_PASS`   | `guest`                                                             | Management API password                                 |
| `PHORCYS_QUORUM_PATH` | `/var/lib/rabbitmq/mnesia/rabbit@localhost/quorum/rabbit@localhost` | Base path of the quorum queue Raft storage on disk      |
| `PHORCYS_BACKUP_DIR`  | `/var/lib/rabbitmq/phorcys-backups`                                 | Directory where queue data is backed up before deletion |

## Building

```bash
go build -o phorcys .
```

## Running

```bash
export PHORCYS_AMQP_URL="amqp://admin:secret@rabbitmq:5672/"
export PHORCYS_MGMT_URL="http://rabbitmq:15672"
export PHORCYS_MGMT_USER="admin"
export PHORCYS_MGMT_PASS="secret"
export PHORCYS_QUORUM_PATH="/var/lib/rabbitmq/mnesia/rabbit@rabbitmq/quorum/rabbit@rabbitmq"
export PHORCYS_BACKUP_DIR="/data/phorcys-backups"

./phorcys
```

Phorcys runs until it receives `SIGINT` or `SIGTERM`.

## Requirements

- RabbitMQ 3.8+ with quorum queues enabled
- The process must run on the **same host** as the RabbitMQ node (for
  filesystem access to Raft data and `rabbitmqctl` fallback eviction)
- Go 1.21+ to build from source

## Project layout

```text
phorcys/
├── main.go                 # Entry point
├── internal/
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

---

## Contributing

Maintainers are accepting patches and contributions to this project.
Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details about submitting
contributions to this project.

---

## Security Policy

Please see our [Security Policy](SECURITY.md) for details.

## License

This project is licensed under the [Apache 2.0 License](LICENSE).

---

## Third-party dependencies

<!-- markdownlint-disable MD013 -->
| Dependency                       | License                                                             |
|----------------------------------|---------------------------------------------------------------------|
| `github.com/DeedleFake/etf`      | [LICENSE](https://github.com/DeedleFake/etf/blob/master/LICENSE)    |
| `github.com/rabbitmq/amqp091-go` | [LICENSE](https://github.com/rabbitmq/amqp091-go/blob/main/LICENSE) |
| `github.com/stretchr/testify`    | [LICENSE](https://github.com/stretchr/testify/blob/master/LICENSE)  |
<!-- markdownlint-enable MD013 -->
