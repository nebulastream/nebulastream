---
title: "Get Started"
description: ""
lead: ""
date: 2020-10-13T15:21:01+02:00
lastmod: 2020-10-13T15:21:01+02:00
draft: false
menu:
  docs:
    parent: "use-nebulastream"
weight: 130
toc: true
---

Welcome to the NebulaStream self-hosted demo guide. This guide will walk you through setting up and running a demo that showcases NebulaStream's capabilities with two different scenarios: keystrokes and system monitoring.
This demo contains multiple queries for trying out the data streaming system NebulaStream. It includes two scenarios:

1. **Keystrokes**: A game similar to testing how fast one can type. It records the keystrokes and sends them to NebulaStream.
2. **System Monitoring**: Monitors the streams of CPU and memory usage.

Both scenarios use simple bash scripts to create a TCP server and send data over TCP to NebulaStream in CSV format.

### Keystrokes

- **Description**: Records keystrokes and sends them to NebulaStream.
- **Data Schema**:
  - `key`: VARSIZED
  - `timestamp`: UINT64

### System Monitoring

- **Description**: Monitors CPU and memory usage.
- **Data Schemas**:
  - CPU Resources:
    - `timestamp`: UINT64
    - `user`: FLOAT64
    - `nice`: FLOAT64
    - `system`: FLOAT64
    - `idle`: FLOAT64
    - `iowait`: FLOAT64
    - `irq`: FLOAT64
    - `softirq`: FLOAT64
    - `steal`: FLOAT64
    - `guest`: FLOAT64
    - `guest_nice`: FLOAT64

  - Memory Resources:
    - `timestamp`: UINT64
    - `MemTotal`: FLOAT64
    - `MemFree`: FLOAT64
    - `MemAvailable`: FLOAT64
    - `Buffers`: FLOAT64
    - `Cached`: FLOAT64
    - `SwapTotal`: FLOAT64
    - `SwapFree`: FLOAT64

## Getting Started

### Running the Demo with Docker Compose
The scripts [run_monitoring_scenario.sh](run_sys_monitoring_scenario.sh) and [run_keystrokes_scenario.sh](run_keystrokes_scenario.sh) bundle the necessary steps from below into an executable script.
Both scripts 



1. Clone the Repository
2. Do the following steps
 ```bash
 git clone https://github.com/nebulastream/nebulastream/
 cd nebulastream/docs/guide/get_started
 bash ./run_monitoring_scenario.sh <query file.yaml>
 ```
   

### Running the Demo from Local Sources
1. Build NebulaStream: Refer to the [Development Guide]( {{< relref "Developer" >}})
2. Start generating data: Copy the following files into [generate_data/](generate_data/)
- [send_keystrokes_to_nebulastream.sh](send_keystrokes_to_nebulastream.sh)
- [sys_resources.sh](sys_resources.sh)
- [write_stdin_to_file.sh](write_stdin_to_file.sh)
```bash
cd generate_data
./sys_resources.sh  # For the system monitoring scenario 
./send_keystrokes_to_nebulastream.sh  # For the keystrokes scenario
./write_stdin_to_file.sh  # For the keystrokes scenario
```
3. Start the NebulaStream Node Worker: `./build_dir/nes-single-node-worker/nes-single-node-worker`
4. Submit a query via Nebuli: `cat scripts/demo/queries/some_query.yaml | ./build_dir/nes-nebuli/nes-nebuli register -x -s localhost:8080`


### Extra information for the keystrokes use-case
It is not possible to open a standard input stream (stdin) and forward it to the data generator service.
Therefore, we have created an additional script that forwards all stdin to a file that the docker-compose service reads and passes on to NebulaStream.
```bash
bash generate_data/write_stdin_to_file.sh
```


### Expected output
When you successfully deploy a query using Docker Compose, you can expect to see a variety of console outputs, which may differ depending on the specific query being executed. 
However, there are common indicators of a successful deployment that you should look for. 
We focus on getting you started with Docker Compose to streamline your experience. 
A successful deployment typically results in a console output similar to the one illustrated in the image below. 
You should see the services starting up in sequence, followed by the execution of your query. 

Upon successful completion, the terminal output for Docker Compose should indicate that the `get_started_nebuli_1` container has exited with code `0`, signifying that the process was completed without errors. 
This visual confirmation helps ensure that your setup is correct and that NebulaStream is functioning as expected, allowing you to proceed with confidence in exploring its capabilities.

<img src = "../SuccessfulDockerComposeGetStarted.png">

