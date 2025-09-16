# Create Compose Script

The create compose script converts a topology definition like this:

```yaml
workers:
  - host: "localhost:9090"
    grpc: "localhost:8080"
    capacity: 10000
```

into a docker compose file

In bash, you can directly invoke it like this:

> docker compose -f <(./create_compose.sh query.yaml) up

assuming you have previously built a recent worker image like this:

> docker compose -f <(./create_compose.sh query.yaml) --profile build-only build

By default, it picks the `cmake-build-debug` build directory which is the default debug cmake build directory, when
using CLion.
You can overwrite this behavior by setting the `CMAKE_BUILD_DIR` environment

This will start multiple nebulastream workers according to the topology definition with their host names.
You cannot access them because you are not in the same networking namespace as the docker compose container.

You can enter the networking namespace using `nsenter`, but you have to get the `pid` of any docker compose container
first.

```bash 
sudo nsenter -t $(docker inspect -f "{{.State.Pid}}" $(docker ps -q | head -n1)) <your-command>
```

```bash
sudo nsenter -t $(
  docker inspect -f "{{.State.Pid}}" $(
    docker ps -q | head -n1
  )
) -n cmake-build-debug/nes-systests/systest/systest -r -n 100 -e large --clusterConfig=nes-systests/configs/topologies/single-node.yaml
```
