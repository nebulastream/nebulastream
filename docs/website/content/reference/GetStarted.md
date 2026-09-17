---
title: "Quick Start"
weight: 30
---

## Install NebulaStream

The recommended way to run NebulaStream is with [Docker](https://www.docker.com/get-started/).
A basic setup consists of two components: a [**NebulaStream worker**](), which executes the queries, and a [**NebulaStream client**](), which is used to submit queries to the worker.


### Run a NebulaStream Worker

Open a terminal and run the following Docker command to start a NebulaStream worker:

<!-- quick-start-run-worker:start -->
```bash
docker run -d --rm \
  --name worker \
  -v "$PWD/output:/output" \
  nebulastream/nes-worker \
  -- --grpc=0.0.0.0:8080
```
<!-- quick-start-run-worker:end -->

This command starts a NebulaStream worker in a Docker container and exposes its gRPC port on all network interfaces at `0.0.0.0:8080`.
Check with `docker ps` if the worker is running.

Queries are managed by the coordinator, `nes-server`. Start one next to the worker; sharing the worker's network
namespace lets it reach the worker at `localhost:8080`, the address the topology file uses:

<!-- quick-start-run-server:start -->
```bash
docker run -d --rm --name server \
  --network container:worker \
  nebulastream/nes-server \
  --listen 127.0.0.1:8081
```
<!-- quick-start-run-server:end -->

<details>
<summary>Expected Output</summary>

```bash
CONTAINER ID   IMAGE                      COMMAND                  CREATED              STATUS              PORTS     NAMES
522ba7470f73   nebulastream/nes-worker   "nes-single-node-wor…"   About a minute ago   Up About a minute             worker
```
Note that the container ID can be different.
</details>


### Run Your First Query

We will use the [NebulaStream CLI]() to submit a query to the running worker.
To do this, the CLI requires a [topology file](), which defines the query configuration and deployment topology.

Create a file called `topology.yaml` and paste the following content:

<!-- quick-start-topology:start -->
```yaml
query: |
  SELECT VALUE * UINT64(2) AS SCALED_VALUE
  FROM GENERATOR_SOURCE
  INTO RESULTS

sinks:
  - name: RESULTS
    host: localhost:8080
    schema:
      - name: SCALED_VALUE
        type: UINT64
    type: File
    config:
      output_format: CSV
      file_path: /output/results.csv
      append: false

logical:
  - name: GENERATOR_SOURCE
    schema:
      - name: VALUE
        type: UINT64

physical:
  - logical: GENERATOR_SOURCE
    host: localhost:8080
    parser_config:
      type: CSV
      field_delimiter: ","
    type: Generator
    source_config:
      generator_rate_type: FIXED
      generator_rate_config: emit_rate 25
      stop_generator_when_sequence_finishes: ALL
      seed: 5
      generator_schema: |
        SEQUENCE UINT64 0 250 1

workers:
  - host: localhost:8080
    data_address: localhost:9090
    max_operators: 10000
```
<!-- quick-start-topology:end -->

### What Does This Query Do?

This topology file describes a query to be executed on a single-node worker.
The query reads records from [GENERATOR_SOURCE](), uses the field VALUE, multiplies it by 2, and writes the result as a new field called SCALED_VALUE into RESULTS, which is `/output/results.csv`.

Check the [topology file format]() for further details on how to structure a topology file.

### Submit the Query

Run the query using the following command:

<!-- quick-start-submit-query:start -->
```bash
  docker run --rm \
    --network container:worker \
    -v "$PWD/topology.yaml:/catalog/topology.yaml:ro" \
    nebulastream/nes-cli \
    -s /catalog/topology.yaml start
```
<!-- quick-start-submit-query:end -->

The CLI registers the topology on the coordinator, submits the query, waits until it runs, and prints the query as
JSON. Its `query.id` identifies the query for the status and stop commands.

<details>
<summary>Example Output of a Query Submission</summary>

```json
[
  {
    "query": {
      "id": 1,
      "name": null,
      "sql": "SELECT VALUE * UINT64(2) AS SCALED_VALUE\nFROM GENERATOR_SOURCE\nINTO RESULTS\n",
      "state": "Running",
      "start_timestamp": "2026-09-16T10:00:00.120Z",
      "stop_timestamp": null,
      "error": null
    },
    "fragments": []
  }
]
```
</details>

### Check the output

After a few seconds, inspect the output:

<!-- quick-start-inspect-output:start -->
```bash
head output/results.csv
```
<!-- quick-start-inspect-output:end -->

The first rows should look like this:

```text
SCALED_VALUE:UINT64:NOT_NULLABLE
0
2
4
6
8
10
12
14
16
```

### What Happened?

We registered a query via `nes-cli` on the coordinator, which deployed it to the worker.
Generator source inside the NebulaStream worker started generating data and sent it to the execution engine for processing.
Results were outputted to a CSV file after processing.

![Logical plan for the Quick Start query]({{< rel "images/quick-start-query-flow.png" >}})

### Stop the Worker

Then you can stop the coordinator and the worker with the following command:

<!-- quick-start-stop-worker:start -->
```bash
docker stop server worker
```
<!-- quick-start-stop-worker:end -->

## Use Docker Compose

You can also use [Docker Compose](https://docs.docker.com/compose/install/) to orchestrate the setup.

First, create a `compose.yaml`:

<!-- quick-start-compose:start -->
```yaml
services:
  worker:
    image: nebulastream/nes-worker
    volumes:
      - ./output:/output
    command: ["--", "--grpc=0.0.0.0:8080"]
    tty: true
    stdin_open: true

  nes-server:
    image: nebulastream/nes-server
    depends_on:
      - worker
    command: ["--listen", "0.0.0.0:8081"]
    healthcheck:
      test: ["CMD", "curl", "-fsS", "http://localhost:8081/v1/health"]
      interval: 1s
      retries: 30

  nes-cli:
    image: nebulastream/nes-cli
    depends_on:
      nes-server:
        condition: service_healthy
    environment:
      NES_COORDINATOR: http://nes-server:8081
    volumes:
      - .:/catalog
    entrypoint: ["/bin/sh", "-c"]
    command: ["tail -f /dev/null"]
```
<!-- quick-start-compose:end -->

The compose file above runs the coordinator next to the worker and keeps the `nes-cli` alive, so that we can check
the query status, and also stop the query later.

### Create New Topology File

Create a new topology file by using the command below.
It uses the existing topology file to create a new one (`compose-topology.yaml`) by replacing the host address, so that the client can access the worker.

<!-- quick-start-create-compose-topology:start -->
```bash
sed 's/localhost/worker/g' topology.yaml > compose-topology.yaml
```
<!-- quick-start-create-compose-topology:end -->

### Run the Setup

Run the docker compose file using the following command:

<!-- quick-start-start-compose:start -->
```bash
docker compose up -d
```
<!-- quick-start-start-compose:end -->

This runs the worker, the coordinator and the nes-cli instances.

<details>
<summary>Expected Output</summary>

```bash
[+] up 3/3
 ✔ Network nes-first-query_default       Created
 ✔ Container nes-first-query-worker-1    Started
 ✔ Container nes-first-query-nes-cli-1   Started
```

</details>

<br>

Check that the containers are running:

<!-- quick-start-list-compose:start -->
```bash
docker compose ps
```
<!-- quick-start-list-compose:end -->

<details>
<summary>Expected Output</summary>

```bash
CONTAINER ID   IMAGE                       COMMAND                  CREATED         STATUS         PORTS     NAMES
5cf4354e7f2c   nebulastream/nes-cli   "/bin/sh -c 'tail -f…"   2 seconds ago   Up 2 seconds             nes-first-query-nes-cli-1
365d32e83391   nebulastream/nes-worker    "nes-single-node-wor…"   3 seconds ago   Up 2 seconds             nes-first-query-nes-worker-1
```

</details>



### Submit the Query

Run the following command to submit the query via `nes-cli`:

<!-- quick-start-submit-compose-query:start -->
```bash
docker compose exec -T nes-cli nes-cli -s /catalog/compose-topology.yaml start
```
<!-- quick-start-submit-compose-query:end -->

The command prints the query as JSON once it runs; its `query.id` is the query ID.
Keep the query ID for status check or stopping the query later.

<details>
<summary>Example Output of a Query Submission</summary>

```json
[
  {
    "query": {
      "id": 1,
      "name": null,
      "sql": "SELECT VALUE * UINT64(2) AS SCALED_VALUE\nFROM GENERATOR_SOURCE\nINTO RESULTS\n",
      "state": "Running",
      "start_timestamp": "2026-09-16T10:00:00.120Z",
      "stop_timestamp": null,
      "error": null
    },
    "fragments": []
  }
]
```
</details>

### Check Status of the Query

If you kept the query ID, check its status by replacing it with the `<query-id>` below:

<!-- quick-start-status-compose-query:start -->
```bash
docker compose exec -T nes-cli nes-cli status <query-id>
```
<!-- quick-start-status-compose-query:end -->

This should return the status of the registered query.

<details>
<summary>Example Output of Query Status Check</summary>

The status is one JSON object per query: the query's own fields, and its fragments, each with the state of the
worker it runs on.

```json
[
  {
    "id": 1,
    "name": null,
    "sql": "SELECT VALUE * UINT64(2) AS SCALED_VALUE\nFROM GENERATOR_SOURCE\nINTO RESULTS\n",
    "state": "Running",
    "start_timestamp": "2026-09-16T10:00:00.120Z",
    "stop_timestamp": null,
    "error": null,
    "fragments": [
      {
        "id": 1,
        "query_id": 1,
        "host_addr": "worker:8080",
        "num_operators": 3,
        "has_source": true,
        "current_state": "Running",
        "desired_state": "Completed",
        "start_timestamp": "2026-09-16T10:00:00.120Z",
        "stop_timestamp": null,
        "error": null,
        "last_observed_at": "2026-09-16T10:00:05.001Z",
        "worker_state": "Active"
      }
    ]
  }
]
```
</details>

### Stop the Query

Stop the query using `nes-cli`:

<!-- quick-start-stop-compose-query:start -->
```bash
docker compose exec -T nes-cli nes-cli stop <query-id>
```
<!-- quick-start-stop-compose-query:end -->

<details>
<summary>Example Output of a Query Stop Command</summary>

The stopped query, as the coordinator recorded it once it terminated:

```json
{
  "DroppedQueries": [
    {
      "id": 1,
      "name": null,
      "sql": "SELECT VALUE * UINT64(2) AS SCALED_VALUE\nFROM GENERATOR_SOURCE\nINTO RESULTS\n",
      "state": "Stopped",
      "start_timestamp": "2026-09-16T10:00:00.120Z",
      "stop_timestamp": "2026-09-16T10:00:12.480Z",
      "error": null
    }
  ]
}
```
</details>

<br>

And check the status again:

<!-- quick-start-status-stopped-compose-query:start -->
```bash
docker compose exec -T nes-cli nes-cli status <query-id>
```
<!-- quick-start-status-stopped-compose-query:end -->

This should return the status of the registered query as `Stopped`.

<details>
<summary>Expected Output</summary>

```json
[
  {
    "id": 1,
    "name": null,
    "sql": "SELECT VALUE * UINT64(2) AS SCALED_VALUE\nFROM GENERATOR_SOURCE\nINTO RESULTS\n",
    "state": "Stopped",
    "start_timestamp": "2026-09-16T10:00:00.120Z",
    "stop_timestamp": "2026-09-16T10:00:12.480Z",
    "error": null,
    "fragments": [
      {
        "id": 1,
        "query_id": 1,
        "host_addr": "worker:8080",
        "num_operators": 3,
        "has_source": true,
        "current_state": "Stopped",
        "desired_state": "Stopped",
        "start_timestamp": "2026-09-16T10:00:00.120Z",
        "stop_timestamp": "2026-09-16T10:00:12.480Z",
        "error": null,
        "last_observed_at": "2026-09-16T10:00:12.480Z",
        "worker_state": "Active"
      }
    ]
  }
]
```
</details>

### Stop Worker and CLI

You can stop everything by shutting down the Compose stack.

<!-- quick-start-stop-compose:start -->
```bash
docker compose down
```
<!-- quick-start-stop-compose:end -->
---
