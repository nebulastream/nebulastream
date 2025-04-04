# REST Server for Interacting with NebulaStream

The coordinator contains a REST server for managing NebulaStream, i.e., managing queries for a start

The server can be started with `build-docker/nes-coordinator/nes-rest_server PORT`. When using docker, we have to expose the port:
```
docker run \
    --workdir $(pwd) \
    -v $(pwd):$(pwd) \
    -p 8006:8006 \
     nebulastream/nes-development:local \
     build-docker/nes-coordinator/nes-rest_server 8006
```

```
curl 0.0.0.0:8006/query
Hello, oatpp!
```
