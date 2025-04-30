# REST Server for Interacting with NebulaStream

The coordinator contains a REST server for managing NebulaStream, i.e., managing queries for a start

The server can be started with `build-docker/nes-coordinator/nes-rest_server PORT CONFIG_FILE`. When using docker, we have to expose the port:
```
docker run \
    --workdir $(pwd) \
    -v $(pwd):$(pwd) \
    -p 8006:8006 \
     nebulastream/nes-development:local \
     build-docker/nes-coordinator/nes-rest_server 8006 query.yml
```

We can submit queries:

```
curl -d '{"code": "SELECT * FROM csv_source WHERE id != 16 INTO csv_sink"}' -X POST http://0.0.0.0:8019/queries
```

Example result:
```
{"query_id":0,"status":"UNKNOWN"}
```
