# Information related to emulating NES
This README describes the basics of starting and controlling a full emulation scenario.

## General prerequisites
This section lists the necessary pieces required by the emulator.

### Emulator of choice
We use [Containernet](https://github.com/containernet/containernet). It is based on Mininet but uses Docker containers for the nodes with the `net.addDocker` command.

For more information, refer to Containernet's API documentation [here](https://github.com/containernet/containernet/wiki/APIs).

#### Installation
Follow the requirements for Containernet installation [here](https://github.com/containernet/containernet/blob/master/README.md). These instructions require Ubuntu 18.04 or earlier as well as Python 3 and Ansible.

Currently, we're waiting for the authors to discuss if they will accept patches to make installation on Ubuntu 20.04 the default. We will update the repo as necessary.

### Image
For the experiments, we use the NES executable image, in our docker hub located [here](https://hub.docker.com/repository/docker/nebulastream/nes-executable-image). Images are auto-pulled as long as Docker is installed on the same system.

#### Installation
The OCI image format from Containernet is the default one used by Docker. For this reason, the Docker daemon must be installed. For specific information, please refer to official Docker documentation [here](https://docs.docker.com/get-docker/).

## Experiments
This section describes how to create experiments and later run them.

### Writing a scenario
The easiest way to start is to use the [scripts](scripts/) folder, copy an example and modify it or check out existing Containernet [examples](https://github.com/containernet/containernet/tree/master/examples).

Currently, the `one_coordinator_three_workers.py` gives a very simple topology of 1 coordinator connected to 3 workers, using `latest` images. The `three_layers.py` uses `ubuntu:trusty` images to create a 3-layered tree of nodes.

In cases of multi-hop or tree-like topologies use the appropriate number of switches, in order to create and connect to the nodes for each isolated subnetwork.

### CLI
Mininet offers the possibility to drop to a built-in CLI for an experiment. This comes with its own set of commands. Use `help` while in CLI mode or `help CMD` to get information for each respective command.

The CLI part can also be completely ommited and the emulator starts with the experiments immediately.

### Running a scenario
Simply `sudo python3 /path/to/scenario.py`. Replace `python3` with `python`, if your system points to python 3 version already.