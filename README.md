# Argus

![Argus watchdog for RabbitMQ](logo.png)

Argus is a self-healing watchdog for RabbitMQ **quorum queues**. It monitors
the broker's internal log exchange for Raft/quorum error signatures, evaluates
whether a flagged queue is truly unrecoverable, and — when it is —
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

| Variable            | Default                                                             | Description                                             |
|---------------------|---------------------------------------------------------------------|---------------------------------------------------------|
| `ARGUS_AMQP_URL`    | `amqp://guest:guest@localhost:5672/`                                | AMQP broker URL                                         |
| `ARGUS_MGMT_URL`    | `http://localhost:15672`                                            | Management API base URL                                 |
| `ARGUS_MGMT_USER`   | `guest`                                                             | Management API username                                 |
| `ARGUS_MGMT_PASS`   | `guest`                                                             | Management API password                                 |
| `ARGUS_QUORUM_PATH` | `/var/lib/rabbitmq/mnesia/rabbit@localhost/quorum/rabbit@localhost` | Base path of the quorum queue Raft storage on disk      |
| `ARGUS_BACKUP_DIR`  | `/var/lib/rabbitmq/argus-backups`                                   | Directory where queue data is backed up before deletion |

## Building

```bash
go build -o argus .
```

## Running

```bash
export ARGUS_AMQP_URL="amqp://admin:secret@rabbitmq:5672/"
export ARGUS_MGMT_URL="http://rabbitmq:15672"
export ARGUS_MGMT_USER="admin"
export ARGUS_MGMT_PASS="secret"
export ARGUS_QUORUM_PATH="/var/lib/rabbitmq/mnesia/rabbit@rabbitmq/quorum/rabbit@rabbitmq"
export ARGUS_BACKUP_DIR="/data/argus-backups"

./argus
```

Argus runs until it receives `SIGINT` or `SIGTERM`.

## Requirements

- RabbitMQ 3.8+ with quorum queues enabled
- The process must run on the **same host** as the RabbitMQ node (for
  filesystem access to Raft data and `rabbitmqctl` fallback eviction)
- Go 1.21+ to build from source

## Project layout

```text
argus/
├── main.go         # Config, entry point, recovery pipeline orchestration
├── monitor.go      # Log monitor — subscribes to amq.rabbitmq.log
├── manager.go      # DiagnosticsManager — RabbitMQ Management API client
├── health.go       # EvaluateQueueHealth — three-gate health classification
├── locate.go       # Find a queue's Raft data directory by decoding ETF meta files
├── carve.go        # Extract AMQP payloads from .segment / .wal files
├── delete.go       # HTTP queue deletion with rabbitmqctl fallback
└── republish.go    # Republish recovered payloads via AMQP
```

## Limitations & caveats

- **Best-effort recovery.** The ETF carver scans raw bytes heuristically.
  Messages that span corrupted sectors may not be recoverable.
- **Single-node access required.** Argus must run on the RabbitMQ node that
  hosts the quorum queue data. In a multi-node cluster, run one Argus instance
  per node.
- **AMQP message metadata is not preserved.** Recovered messages are
  republished as `application/octet-stream` with `DeliveryMode=Persistent`.
  Original headers, content-type, and routing metadata are not reconstructed.
- **Duplicate delivery is possible.** If a message was already acknowledged
  before the crash, carving may recover and republish it again.

## Future

- Eliminate the limitation where original headers, content-type and routing
  metadata are not recovered.
- Ensure only one node in the RabbitMQ cluster attempts recovery of a queue.
- Manual mode to specify recovery of a specific queue.
- Remove orphaned queues.
- Possible classic queue support.

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
