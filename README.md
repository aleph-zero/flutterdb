<p align="center">
    <a href="https://trino.io/"><img alt="FlutterDB" src=".github/flutterdb.png" style="height:50%;width:30%"/></a>
</p>
<p align="center">
    <b>FlutterDB is an experimental, distributed SQL query engine for <i>small</i> data analytics</b>
</p>

## Overview

FlutterDB is a distributed SQL query engine written in Go. It provides a SQL interface for querying JSON documents stored across a cluster of nodes. The engine includes a custom SQL parser, logical query planner with optimizations, and physical execution engine backed by the Bluge full-text search library.

## Features

- **SQL Query Support**: SELECT, CREATE TABLE, SHOW TABLES with WHERE, LIMIT clauses
- **Filter Predicates**: Supports =, !=, <, <=, >, >=, LIKE, AND, OR, NOT operators
- **Data Types**: TEXT, KEYWORD, INTEGER, FLOAT, GEOPOINT, DATETIME
- **Table Partitioning**: Partition tables by a column for distributed data storage
- **Cluster Membership**: Built-in cluster coordination using Serf/Memberlist
- **Interactive CLI**: REPL client with command history
- **Observability**: OpenTelemetry integration for tracing and metrics
- **Kubernetes Ready**: Helm chart for deployment on Kubernetes

## Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│    Client    │────▶│   HTTP API   │────▶│ Query Engine │
└──────────────┘     └──────────────┘     └──────────────┘
                                                 │
                     ┌───────────────────────────┼───────────────────────────┐
                     │                           │                           │
                     ▼                           ▼                           ▼
              ┌────────────┐              ┌────────────┐              ┌────────────┐
              │   Lexer    │─────────────▶│   Parser   │─────────────▶│  Resolver  │
              └────────────┘              └────────────┘              └────────────┘
                                                                            │
                     ┌───────────────────────────┼───────────────────────────┘
                     │                           │
                     ▼                           ▼
              ┌────────────┐              ┌────────────┐
              │  Logical   │─────────────▶│  Physical  │
              │   Plan     │  Optimizer   │   Plan     │
              └────────────┘              └────────────┘
                                                 │
                                                 ▼
                                          ┌────────────┐
                                          │   Index    │
                                          │  (Bluge)   │
                                          └────────────┘
```

## Installation

### Build from Source

```bash
# Clone the repository
git clone https://github.com/aleph-zero/flutterdb.git
cd flutterdb

# Build the binary
make build
```

### Docker

```bash
make docker TAG=0.0.1
```

## Usage

### Start the Server

```bash
./flutterdb server --config flutterdb.yaml
```

Server options:
- `--server.addr`: Address to bind (default: `0.0.0.0`)
- `--server.port`: Port to listen on (default: `1234`)
- `--cluster.node-name`: Unique node identifier (default: hostname)
- `--cluster.membership-listen-addr`: Cluster membership address (default: `127.0.0.1`)
- `--cluster.membership-listen-port`: Cluster membership port (default: `5678`)
- `--cluster.membership-join-addrs`: Addresses of existing cluster nodes to join
- `--metastore.data-dir`: Directory for metastore data (default: `.metastore`)

### Connect with the Client

```bash
./flutterdb client --client.remote-addr 127.0.0.1 --client.remote-port 1234
```

This starts an interactive REPL:

```
flutterdb> SHOW TABLES
flutterdb> SELECT * FROM books WHERE author = 'Leo Tolstoy'
flutterdb> SELECT title, author FROM books WHERE published > '1900-01-01' LIMIT 10
```

## SQL Syntax

### CREATE TABLE

```sql
CREATE TABLE books (
    title TEXT,
    author KEYWORD,
    published DATETIME,
    price FLOAT
) PARTITION BY author
```

Supported column types:
- `TEXT` - Full-text searchable string
- `KEYWORD` - Exact match string
- `INTEGER` - 64-bit integer
- `FLOAT` - 64-bit floating point
- `GEOPOINT` - Geographic coordinates
- `DATETIME` - Date and time

### SELECT

```sql
SELECT * FROM table_name
SELECT col1, col2 FROM table_name WHERE condition LIMIT n
```

### SHOW TABLES

```sql
SHOW TABLES
```

### WHERE Clause Operators

| Operator | Description |
|----------|-------------|
| `=` | Equal |
| `!=` | Not equal |
| `>` | Greater than |
| `>=` | Greater than or equal |
| `<` | Less than |
| `<=` | Less than or equal |
| `LIKE` | Pattern matching |
| `AND` | Logical AND |
| `OR` | Logical OR |
| `NOT` | Logical negation |

## Configuration

Configuration can be provided via YAML file (`--config`) or command-line flags.

Example `flutterdb.yaml`:

```yaml
server:
  addr: "0.0.0.0"
  port: 1234

cluster:
  membership-listen-addr: "127.0.0.1"
  membership-listen-port: 5678
  membership-join-addrs: []

metastore:
  data-dir: "/var/lib/flutterdb/metastore"

client:
  remote-addr: "127.0.0.1"
  remote-port: 1234
```

## HTTP API

### Query Endpoint

```bash
curl "http://localhost:1234/sql?q=SELECT%20*%20FROM%20books"
```

### Index Documents

```bash
curl -X POST "http://localhost:1234/index/books" \
  -H "Content-Type: application/x-ndjson" \
  --data-binary @documents.ndjson
```

### Create Table

```bash
curl -X POST "http://localhost:1234/metastore" \
  -H "Content-Type: application/json" \
  -d '{
    "table": "books",
    "columns": {
      "title": {"column": "title", "type": "text"},
      "author": {"column": "author", "type": "keyword"}
    }
  }'
```

### Cluster Endpoints

- `GET /identity` - Get node identity
- `GET /membership` - List cluster members
- `GET /cluster` - Get cluster info

## Deployment

### Kubernetes with Helm

```bash
# Create a kind cluster
make kind-cluster

# Deploy with Helm
make kind-deploy-helm
```

The Helm chart is located in `deploy/flutterdb/` and supports:
- StatefulSet deployment for stable network identities
- Headless service for cluster membership
- Configurable replicas and resources

### Docker Compose

```bash
docker compose -f deploy/compose.yaml up
```

## Development

### Run Tests

```bash
go test ./...
```

### Project Structure

```
flutterdb/
├── api/          # HTTP API handlers
├── client/       # CLI client
├── cmd/          # Cobra command definitions
├── deploy/       # Deployment configs (Docker, Helm, k8s)
├── engine/       # Query engine
│   ├── ast/      # Abstract syntax tree
│   ├── evaluator/# Expression evaluator
│   ├── logical/  # Logical query plan
│   ├── parser/   # SQL lexer and parser
│   ├── physical/ # Physical execution plan
│   ├── token/    # Token definitions
│   └── types/    # Data type definitions
├── server/       # Server bootstrap
├── service/      # Core services
│   ├── cluster/  # Cluster coordination
│   ├── identity/ # Node identity
│   ├── index/    # Document indexing (Bluge)
│   ├── membership/ # Cluster membership (Serf)
│   ├── metastore/  # Table metadata storage
│   └── query/    # Query execution service
├── telemetry/    # OpenTelemetry setup
└── testdata/     # Test fixtures
```
