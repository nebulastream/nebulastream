#!/bin/bash

/bin/prometheus-node-exporter > prometheus.log 2>&1 &

sleep 20 && exec /entrypoint.sh $@ > nes-runtime.log 2>&1 &