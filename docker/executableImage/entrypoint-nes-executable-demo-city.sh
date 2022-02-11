#!/bin/bash
# Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#quit if command returns non-zero code
set -e

if [ $# -eq 0 ]
then
    /opt/local/nebula-stream/nesCoordinator --coordinatorIp=$coordinatorIp --rpcPort=$rpcPort --restPort=$restPort --restPort=$restPort \
                                            --enableSemanticQueryValidation=$enableSemanticQueryValidation --enableMonitoring=$enableMonitoring &
    sleep 5s

    curl -X POST \
      http://localhost:8000/v1/nes/sourceCatalog/addLogicalStream \
      -H 'cache-control: no-cache' \
      -H 'content-type: application/json' \
      -H 'postman-token: c74ed295-f367-c818-b52a-ce1cc0ea52b1' \
      -d '{ "logicalSourceName": "nesCityHospital", "schema": "Schema::create()->addField(\"type\",DataTypeFactory::createFixedChar(12))->addField(\"hospitalId\",INT64)->addField(\"stationId\",INT64)->addField(\"patientId\",INT64)->addField(\"time\",UINT64)->addField(\"healthStatus\",INT8)->addField(\"healthStatusDuration\",INT64)->addField(\"recovered\",BOOLEAN)->addField(\"dead\",BOOLEAN);" }'&

    sleep 5s

    /opt/local/nebula-stream/nesWorker --coordinatorPort=$coordinatorPort
                                       --rpcPort=$rpcPort
                                       --rpcPort=$rpcPort
                                       --physicalSourceName=$physicalSourceName
                                       --logicalSourceName=$logicalSourceName
                                       --url=$url
                                       --clientId=$clientId
                                       --userName=$userName
                                       --topic=$topic
                                       --inputFormat=$inputFormat
                                       --qos=$qos
                                       --cleanSession=$cleanSession
                                       --flushIntervalMS=$flushIntervalMS
                                       --type=$type
                                       --numWorkerThreads=$numWorkerThreads
                                       --enableMonitoring=$enableMonitoring
else
    exec $@
fi
