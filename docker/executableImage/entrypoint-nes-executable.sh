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



if [ $# -eq 0 ]
then
    /opt/local/nebula-stream/nesCoordinator --serverIp=$serverIp --restPort=$restPort --coordinatorPort=$coordinatorPort \
                                            --enableQueryMerging=$enableQueryMerging &
    sleep 5s
    /opt/local/nebula-stream/nesWorker --coordinatorPort=$coordinatorPort --sourceType=$sourceType --sourceConfig=$sourceConfig \
                                        --numberOfBuffersToProduce=$numberOfBuffersToProduce --sourceFrequency=$sourceFrequency \
                                        --physicalStreamName=$physicalStreamName --logicalStreamName=$logicalStreamName \
                                        --skipHeader=$skipHeader
else
    exec $@
fi
