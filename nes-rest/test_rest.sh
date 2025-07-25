#! /usr/bin/bash
set -e

curl -X 'POST' \
  'http://localhost:8081/api/v1/sources' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "source_name": "sensor_data",
  "schema": [
    {
      "field_name": "sensor_id",
      "field_type": "UINT64"
    },
    {
      "field_name": "temperature",
      "field_type": "FLOAT64"
    },
    {
      "field_name": "humidity",
      "field_type": "FLOAT64"
    },
    {
      "field_name": "timestamp",
      "field_type": "UINT64"
    },
    {
      "field_name": "location",
      "field_type": "VARSIZED"
    }
  ]
}' | jq

curl -X 'POST' \
  'http://localhost:8081/api/v1/sources/sensor_data/physical' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "worker_id": 0,
  "connector": "File",
  "formatter": "CSV",
  "config": {
    "filePath": "/data/sensor_readings.csv"
  }
}' | jq

curl -X 'POST' \
  'http://localhost:8081/api/v1/sinks' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "sink_name": "alert_sink",
  "worker_id": 0,
  "connector": "File",
  "config": {
    "filePath": "/tmp/alerts.json",
    "inputFormat": "CSV",
    "append": "false"
  }
}' | jq

curl -X 'POST' \
  'http://localhost:8081/api/v1/queries' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "code": "SELECT * FROM sensor_data WHERE temperature > FLOAT32(25.0) INTO alert_sink"
}' | jq

curl -X 'GET' \
  'http://localhost:8081/api/v1/queries' \
  -H 'accept: application/json' | jq

curl -X 'GET' \
  'http://localhost:8081/api/v1/queries' \
  -H 'accept: application/json' | jq

curl -X 'GET' \
  'http://localhost:8081/api/v1/queries' \
  -H 'accept: application/json' | jq
