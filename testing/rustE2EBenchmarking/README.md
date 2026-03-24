# NES Benchmark Framework

A comprehensive benchmarking framework for the NebulaStream (NES) system that automates the execution of complex benchmark scenarios with TCP data sources.

## Features

- **TOML-based configuration**: Define benchmarks in readable configuration files
- **Automated TCP server management**: Automatically starts and manages TCP servers for data streaming
- **Query orchestration**: Submit queries sequentially or in parallel with configurable delays
- **System lifecycle management**: Automatic startup and shutdown of the NES system
- **Metrics collection**: Gather and save performance metrics throughout the benchmark
- **Batch execution**: Run multiple benchmark configurations from a directory

## Project Structure

```
nes-benchmark/
├── Cargo.toml              # Project dependencies
├── src/
│   ├── main.rs            # CLI entry point
│   ├── config.rs          # Configuration structures
│   ├── runner.rs          # Benchmark execution logic
│   ├── tcp_manager.rs     # TCP server management
│   ├── query_manager.rs   # Query submission handling
│   ├── system_manager.rs  # NES system lifecycle
│   └── tcp_server.rs      # Embedded TCP server
├── benchmarks/            # Benchmark configuration files
│   └── example.toml
└── README.md
```

## Building the Project

### Prerequisites

1. **Rust**: Install Rust and Cargo (https://rustup.rs/)
2. **NES System**: Ensure you have the NES binaries built:
    - `nes-single-node-worker`
    - `nes-nebuli`

### Build Steps

```bash
# Clone/create the project
mkdir nes-benchmark
cd nes-benchmark

# Create the project structure
cargo init

# Replace Cargo.toml with the provided one
# Copy all src/*.rs files to the src/ directory

# Build the project in release mode for optimal performance
cargo build --release

# The binaries will be in target/release/
# - nes-benchmark (main benchmark runner)
# - tcp-server (TCP data server)
```

## Configuration

### Creating a Benchmark Configuration

Create a TOML file (e.g., `benchmark.toml`) with the following structure:

```toml
name = "My Benchmark"
description = "Testing query performance"

[system]
binary_path = "/path/to/nes-single-node-worker"
working_directory = "/path/to/working/dir"
startup_delay_ms = 5000
shutdown_delay_ms = 2000

[system.parameters]
"worker.queryEngine.numberOfWorkerThreads" = "23"
"worker.queryEngine.admissionQueueSize" = "5000"
"worker.queryEngine.taskQueueSize" = "100000"
"worker.defaultQueryExecution.executionMode" = "COMPILER"
"worker.numberOfBuffersInGlobalBufferManager" = "800000"
"worker.bufferSizeInBytes" = "65536"

[[tcp_servers]]
name = "yahoo_server"
host = "127.0.0.1"
port = 1111
file_path = "/path/to/yahoo_data.csv"
repeat_count = 1
buffer_size = 65536
batch_size = 1

[[queries]]
id = "projection_query"
yaml_path = "/path/to/q-projection_yahoo_tcp.yaml"
tcp_sources = ["yahoo_server"]
output_suffix = "projection"

[[benchmark_sequence]]
type = "StartSystem"

[[benchmark_sequence]]
type = "Wait"
duration_ms = 5000

[[benchmark_sequence]]
type = "StartTcpServers"
servers = ["yahoo_server"]

[[benchmark_sequence]]
type = "SubmitQuery"
query_id = "projection_query"
count = 5
delay_between_ms = 100

[[benchmark_sequence]]
type = "Wait"
duration_ms = 30000

[[benchmark_sequence]]
type = "CollectMetrics"
name = "final_metrics"

[[benchmark_sequence]]
type = "StopSystem"

[output]
results_dir = "./benchmark_results"
log_dir = "./benchmark_logs"
metrics_file = "./metrics.json"
save_query_outputs = true
```

## Usage

### Basic Commands

```bash
# Generate an example configuration
./target/release/nes-benchmark example --output my_benchmark.toml

# Validate a configuration
./target/release/nes-benchmark validate --path my_benchmark.toml

# Run a single benchmark
./target/release/nes-benchmark run --config my_benchmark.toml

# Dry run (validation only, no execution)
./target/release/nes-benchmark run --config my_benchmark.toml --dry-run

# Run all benchmarks in a directory
./target/release/nes-benchmark run-dir --dir ./benchmarks/

# Validate all configurations in a directory
./target/release/nes-benchmark validate --path ./benchmarks/
```

### Benchmark Sequence Steps

The framework supports the following step types in the `benchmark_sequence`:

1. **StartSystem**: Start the NES system with configured parameters
2. **StopSystem**: Stop the NES system
3. **StartTcpServers**: Start specified TCP servers
4. **StopTcpServers**: Stop specified TCP servers
5. **SubmitQuery**: Submit a query N times with optional delay
6. **SubmitParallel**: Submit multiple queries in parallel
7. **Wait**: Wait for a specified duration
8. **WaitForCompletion**: Wait for queries to complete
9. **CollectMetrics**: Collect and save current metrics

### Example Workflow

1. **Prepare your data files**: Ensure all CSV/data files referenced in the configuration exist

2. **Create query YAML files**: Prepare your NES query definitions

3. **Write benchmark configuration**: Create a TOML file defining your benchmark scenario

4. **Validate configuration**:
   ```bash
   ./target/release/nes-benchmark validate --path my_benchmark.toml
   ```

5. **Run the benchmark**:
   ```bash
   ./target/release/nes-benchmark run --config my_benchmark.toml
   ```

6. **Check results**: Results will be saved in the configured output directories

### Advanced Usage

#### Running Multiple Queries with Different TCP Requirements

```toml
[[queries]]
id = "join_query"
yaml_path = "/path/to/join_query.yaml"
tcp_sources = ["source1", "source2"]  # This query needs 2 TCP sources

[[benchmark_sequence]]
type = "SubmitQuery"
query_id = "join_query"
count = 5  # Will require 10 total TCP connections (2 per query × 5)
```

#### Parallel Query Submission

```toml
[[benchmark_sequence]]
type = "SubmitParallel"
queries = [
    { query_id = "query1", count = 3 },
    { query_id = "query2", count = 5 }
]
```

## Integration with Existing Code

The framework seamlessly integrates your existing components:

1. **TCP Server**: The framework uses your existing TCP server code, managing it automatically based on query requirements
2. **Query Submission**: Uses your Python script logic, reimplemented in Rust for better integration
3. **System Management**: Handles the NES system lifecycle with proper parameter passing

## Troubleshooting

### Common Issues

1. **TCP Server Connection Issues**
    - Ensure ports are not already in use
    - Check that data files are accessible
    - Verify the batch_size matches expected connections

2. **Query Submission Failures**
    - Verify NES system is running (check logs)
    - Ensure query YAML files are valid
    - Check that output directories are writable

3. **System Startup Problems**
    - Verify binary paths are correct
    - Check system parameters are valid
    - Ensure sufficient resources are available

### Debug Mode

Enable detailed logging by setting the environment variable:
```bash
RUST_LOG=debug ./target/release/nes-benchmark run --config my_benchmark.toml
```

## Performance Tips

1. **Build in Release Mode**: Always use `cargo build --release` for benchmarks
2. **TCP Buffer Sizes**: Adjust buffer_size based on your data characteristics
3. **Batch Sizes**: Configure batch_size to match your parallelism requirements
4. **System Parameters**: Tune NES parameters based on your hardware

## Example Benchmark Scenarios

### Sequential Processing
```toml
# Submit queries one by one with delays
[[benchmark_sequence]]
type = "SubmitQuery"
query_id = "query1"
count = 1
delay_between_ms = 0

[[benchmark_sequence]]
type = "Wait"
duration_ms = 5000

[[benchmark_sequence]]
type = "SubmitQuery"
query_id = "query2"
count = 1
```

### Stress Testing
```toml
# Submit many queries in parallel
[[benchmark_sequence]]
type = "SubmitParallel"
queries = [
    { query_id = "heavy_query", count = 20 },
    { query_id = "light_query", count = 50 }
]
```

### Phased Benchmark
```toml
# Warm-up phase
[[benchmark_sequence]]
type = "SubmitQuery"
query_id = "warmup_query"
count = 5
delay_between_ms = 1000

[[benchmark_sequence]]
type = "CollectMetrics"
name = "after_warmup"

# Main benchmark phase
[[benchmark_sequence]]
type = "SubmitParallel"
queries = [
    { query_id = "main_query", count = 10 }
]

[[benchmark_sequence]]
type = "CollectMetrics"
name = "after_main"
```

## Output Files

The framework generates several output files:

1. **Query Results**: `{results_dir}/{benchmark_name}_{query_id}_{instance}_{timestamp}.csv`
2. **Metrics**: `{metrics_file}` - JSON file with collected metrics
3. **Logs**: `{log_dir}/benchmark_{timestamp}.log`

## Extending the Framework

To add new functionality:

1. **New Step Types**: Add variants to `BenchmarkStep` enum in `config.rs`
2. **New Metrics**: Extend `collect_current_metrics()` in `runner.rs`
3. **Custom Validation**: Add checks to `BenchmarkConfig::validate()`

## Migration from Python Script

Your original Python script functionality is now integrated:

| Python Script | Framework Equivalent |
|--------------|---------------------|
| YAML modification | Automatic in `query_manager.rs` |
| Query registration | Handled by `QueryManager::submit_query()` |
| Query starting | Handled by `QueryManager::submit_query()` |
| Multiple queries | Use `count` parameter or `SubmitParallel` |

## Complete Example

Here's a complete example matching your original use case:

```toml
name = "Yahoo Projection Benchmark"
description = "Testing projection queries on Yahoo dataset"

[system]
binary_path = "/home/rudi/dima/nebulastream-public/cmake-build-relwithdebinfo/nes-single-node-worker/nes-single-node-worker"
working_directory = "/home/rudi/dima/nebulastream-public"
startup_delay_ms = 3000

[system.parameters]
"worker.queryEngine.numberOfWorkerThreads" = "23"
"worker.queryEngine.admissionQueueSize" = "5000"
"worker.queryEngine.taskQueueSize" = "100000"
"worker.defaultQueryExecution.executionMode" = "COMPILER"
"worker.numberOfBuffersInGlobalBufferManager" = "800000"
"worker.bufferSizeInBytes" = "65536"

[[tcp_servers]]
name = "yahoo_tcp"
host = "localhost"
port = 1111
file_path = "/home/rudi/dima/yahoo_data.csv"
repeat_count = 1
buffer_size = 65536

[[queries]]
id = "projection_blocking"
yaml_path = "/home/rudi/dima/nebulastream-public/testing/q-projection_yahoo_tcp_blocking.yaml"
tcp_sources = ["yahoo_tcp"]

[[benchmark_sequence]]
type = "StartSystem"

[[benchmark_sequence]]
type = "Wait"
duration_ms = 3000

[[benchmark_sequence]]
type = "StartTcpServers"
servers = ["yahoo_tcp"]

[[benchmark_sequence]]
type = "SubmitQuery"
query_id = "projection_blocking"
count = 10
delay_between_ms = 100

[[benchmark_sequence]]
type = "WaitForCompletion"
timeout_ms = 60000

[[benchmark_sequence]]
type = "StopSystem"

[output]
results_dir = "/home/rudi/dima/nebulastream-public/testing/output"
log_dir = "/home/rudi/dima/nebulastream-public/testing/logs"
metrics_file = "/home/rudi/dima/nebulastream-public/testing/metrics.json"
save_query_outputs = true
```

Save this as `yahoo_benchmark.toml` and run:
```bash
./target/release/nes-benchmark run --config yahoo_benchmark.toml
```

## License

This framework is provided as-is for benchmarking purposes.