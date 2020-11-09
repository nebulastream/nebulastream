/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/


#include <Network/PartitionManager.hpp>
#include <Util/Logger.hpp>

namespace NES::Network {

uint64_t PartitionManager::registerSubpartition(NesPartition partition) {
    std::unique_lock lock(partitionCounterMutex);
    //check if partition is present
    if (partitionCounter.find(partition) != partitionCounter.end()) {
        // partition is contained
        partitionCounter[partition] = partitionCounter[partition] + 1;
    } else {
        partitionCounter[partition] = 0;
    }
    NES_DEBUG("PartitionManager: Registering " << partition.toString() << "=" << partitionCounter[partition]);
    return partitionCounter[partition];
}

uint64_t PartitionManager::unregisterSubpartition(NesPartition partition) {
    std::unique_lock lock(partitionCounterMutex);

    if (partitionCounter.find(partition) == partitionCounter.end()) {
        NES_FATAL_ERROR("PartitionManager: error while unregistering partition " << partition << " reason: partition not found");
        NES_THROW_RUNTIME_ERROR("PartitionManager: error while unregistering partition");
    }

    // if partition is contained, decrement counter
    auto counter = partitionCounter[partition];

    if (counter == 0) {
        //if counter reaches 0, log error
        NES_INFO("PartitionManager: Deleting " << partition.toString() << ", counter is at 0.");
        partitionCounter.erase(partition);
    } else {
        //else decrement counter
        partitionCounter[partition]--;
        NES_INFO("PartitionManager: Unregistering " << partition.toString() << "; newCnt(" << counter << ")");
    }

    // return new counter
    return counter;
}

uint64_t PartitionManager::getSubpartitionCounter(NesPartition partition) {
    std::shared_lock lock(partitionCounterMutex);
    return partitionCounter.at(partition);
}

uint64_t PartitionManager::deletePartition(NesPartition partition) {
    std::unique_lock lock(partitionCounterMutex);
    NES_INFO("PartitionManager: Deleting " << partition.toString());
    return partitionCounter.erase(partition);
}

void PartitionManager::clear() {
    std::unique_lock lock(partitionCounterMutex);
    NES_INFO("PartitionManager: Clearing registered partitions");
    partitionCounter.clear();
}

bool PartitionManager::isRegistered(NesPartition partition) const {
    //check if partition is present
    std::shared_lock lock(partitionCounterMutex);
    return partitionCounter.find(partition) != partitionCounter.end();
}

}// namespace NES::Network