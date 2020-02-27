#!/bin/bash

if [ $# -eq 0 ]
then
    /opt/local/nebula-stream/nesCoordinator --rest_host=127.0.0.1 --actor_port=12346 &
    sleep 5s
    /opt/local/nebula-stream/nesWorker --actor_port=12346 --sourceType=CSVSource --sourceConfig=/opt/local/nebula-stream/exdra.csv --numberOfBuffersToProduce=100 --sourceFrequency=1 --physicalStreamName=test_stream --logicalStreamName=exdra
else
    exec $@
fi
