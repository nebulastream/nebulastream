# Usage

> cat q.yaml | docker run --rm -i -v $(pwd):/output --user $(id -u) unikernel-export:latest -

This will run the exporter on the `q.yaml` file containing the query and topology.
The exporter will write its output into the current working directory.

# Example Query
```yaml
query: |
  Query::from("input1").joinWith(Query::from("input2"))
                                  .where(Attribute("id")).equalsTo(Attribute("id"))
                                  .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(500)))
                             .joinWith(Query::from("input3"))
                                  .where(Attribute("id")).equalsTo(Attribute("id"))
                                  .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(500)))
                             .sink(NullOutputSinkDescriptor::create());
topology:
  sink:
    node:
      ip: 127.0.0.1
      port: 8085
      resources: 1
  workers:
    - node:
        ip: 127.0.0.1
        port: 8080
        resources: 90
      sources:
        - name: input1
          tcp:
            ip: 127.0.0.1
            port: 8091
          schema:
            fields:
              - type: INT64
                name: id
              - type: INT64
                name: timestamp
      links:
        - sink
    - node:
        ip: 127.0.0.1
        port: 8081
        resources: 1
      links:
        - '2'
      sources:
        - name: input2
          tcp:
            ip: 127.0.0.1
            port: 8090
          schema:
            fields:
              - type: INT64
                name: id
              - type: INT64
                name: timestamp
    - node:
        ip: 127.0.0.1
        port: 8084
        resources: 1
      links:
        - '2'
      sources:
        - name: input3
          tcp:
            ip: 127.0.0.1
            port: 8092
          schema:
            fields:
              - type: INT64
                name: id
              - type: INT64
                name: timestamp

```