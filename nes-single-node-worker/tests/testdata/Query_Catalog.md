# Description
This file maps binary queries that are fully-specified and serialized queries to the original queries.

## query_single_csv_source.bin
```yaml
query: |
  Query::from("default_source")
    .filter(Attribute("id") < 32)
    .sink(PrintSinkDescriptor::create());
logical:
  - name: default_source
    schema:
      - name: id
        type: UINT64
physical:
  - logical: default_source
    config:
      type: CSV_SOURCE
      filePath: /home/rudi/dima/nebulastream/nes-coordinator/tests/TestData/car.csv
      delimiter: "*"
      numberOfBuffersToProduce: 32
      gatheringInterval: 1
```