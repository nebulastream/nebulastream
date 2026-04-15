# Smart Manufacturing / Process Intelligence

This repository contains a demo setup for [NebulaStream](https://nebula.stream/) showing a use-case for Smart Manufacturing. 

Three video sources are modeled that show engine wiring, pipe clipping, and underbody screws from the [AutoVI dataset](https://auto-vi.utc.fr/index.html).

NebulaStream reads these video sources, 
uses an ML inference operator to detect anomalies in the images (i.e. manufacturing errors) 
and sends the detected anomalies to a Kafka Sink. 

For better insight into the demo, we further provide a simple WebApp that can show the three video streams. 

Everything is set up and connected using docker compose and works out of the box if you have docker installed.

## How to Run

### Start system

```bash 
docker compose up --force-recreate 
```


### Set up Kafka
```bash
## Initiate Topic 
./demo/scripts/00_kafka-init-topic.sh
### Show Kafka output  
./demo/scripts/01_kafka-consume.sh
```

### Start NebulaStream Queries
```bash
 ./demo/scripts/02_start_query.sh ./demo/queries/query-engine-wiring.yaml 
 ./demo/scripts/02_start_query.sh ./demo/queries/query-pipe-staple.yaml 
 ./demo/scripts/02_start_query.sh ./demo/queries/query-underbody-screw.yaml
```

### Start Browser Viewer

1. Navigate browser to `localhost:8085`
2. Enter the following three configurations to "Connections" and press "Add connection" each time. 

    | Host                     | Port   |
    | ---                      | ---    |
    | source-engine-wiring   | 8081 |
    | source-pipe-staple     | 8081 |
    | source-underbody-screw | 8081 |
3. Draw the three now visible screens to the wished size


## References
* AutoVI dataset: Carvalho et al., The Automotive Visual Inspection Dataset (AutoVI): A Genuine Industrial Production Dataset for Unsupervised Anomaly Detection, 2024 (https://doi.org/10.1016/j.compind.2024.104151) 