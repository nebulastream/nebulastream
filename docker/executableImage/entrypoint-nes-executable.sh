#!/bin/bash

if [ $# -eq 0 ]
then
    /opt/local/nebula-stream/nesCoordinator --serverIp=$serverIp --restPort=$restPort --coordinatorPort=$coordinatorPort \
                                            --enableQueryMerging=$enableQueryMerging &
    sleep 5s
    /opt/local/nebula-stream/nesWorker --coordinatorPort=$coordinatorPort --sourceType=$sourceType --sourceConfig=$sourceConfig \
                                        --numberOfBuffersToProduce=$numberOfBuffersToProduce --sourceFrequency=$sourceFrequency \
                                        --physicalStreamName=$physicalStreamName --logicalStreamName=$logicalStreamName
else
    exec $@
fi
