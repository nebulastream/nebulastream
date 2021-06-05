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
#include <Runtime/Execution/DataEmitter.hpp>
#include <Util/Logger.hpp>
#include <utility>

namespace NES::Network {

PartitionManager::PartitionEntry::PartitionEntry(DataEmitterPtr emitter) : emitter(std::move(emitter)) {
    // nop
}

uint64_t PartitionManager::PartitionEntry::count() const { return partitionCounter; }

void PartitionManager::PartitionEntry::pin() { partitionCounter++; }

void PartitionManager::PartitionEntry::unpin() { partitionCounter--; }

DataEmitterPtr PartitionManager::PartitionEntry::getEmitter() { return emitter; }

void PartitionManager::pinSubpartition(NesPartition partition) {
    std::unique_lock lock(mutex);
    auto it = partitions.find(partition);
    if (it != partitions.end()) {
        it->second.pin();
        return;
    }
    NES_ASSERT2_FMT(false, "Cannot increment partition counter as partition does not exists " << partition);
}

void PartitionManager::unpinSubpartition(NesPartition partition) {
    std::unique_lock lock(mutex);
    auto it = partitions.find(partition);
    if (it != partitions.end()) {
        it->second.unpin();
        return;
    }
    NES_ASSERT2_FMT(false, "Cannot increment partition counter as partition does not exists " << partition);

}

bool PartitionManager::registerSubpartition(NesPartition partition, DataEmitterPtr emitterPtr) {
    std::unique_lock lock(mutex);
    //check if partition is present
    auto it = partitions.find(partition);
    if (it != partitions.end()) {
        // partition is contained
        it->second.pin();
    } else {
        partitions[partition] = PartitionEntry(std::move(emitterPtr));
    }
    NES_DEBUG("PartitionManager: Registering " << partition.toString() << "=" << partitions[partition].count());
    return partitions[partition].count() == 0;
}

bool PartitionManager::unregisterSubpartition(NesPartition partition) {
    std::unique_lock lock(mutex);

    auto it = partitions.find(partition);
    NES_ASSERT2_FMT(it != partitions.end(),
                    "PartitionManager: error while unregistering partition " << partition << " reason: partition not found");

    // safeguard
    if (it->second.count() == 0) {
        NES_INFO("PartitionManager: Deleting " << partition.toString() << ", counter is at 0.");
        partitions.erase(it);
        return true;
    }

    it->second.unpin();
    NES_INFO("PartitionManager: Unregistering " << partition.toString() << "; newCnt(" << it->second.count() << ")");
    if (it->second.count() == 0) {
        //if counter reaches 0, log error
        NES_INFO("PartitionManager: Deleting " << partition.toString() << ", counter is at 0.");
        partitions.erase(it);
        return true;
    }
    return false;
}

uint64_t PartitionManager::getSubpartitionCounter(NesPartition partition) {
    std::shared_lock lock(mutex);
    return partitions[partition].count();
}

DataEmitterPtr PartitionManager::getDataEmitter(NesPartition partition) {
    std::unique_lock lock(mutex);
    return partitions[partition].getEmitter();
}

void PartitionManager::clear() {
    std::unique_lock lock(mutex);
    NES_INFO("PartitionManager: Clearing registered partitions");
    partitions.clear();
}

bool PartitionManager::isRegistered(NesPartition partition) const {
    //check if partition is present
    std::shared_lock lock(mutex);
    return partitions.find(partition) != partitions.end();
}


}// namespace NES::Network