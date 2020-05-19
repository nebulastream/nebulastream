
#include <Util/Logger.hpp>
#include <Network/PartitionManager.hpp>

namespace NES::Network {

uint64_t PartitionManager::registerSubpartition(NesPartition partition) {
    unique_lock<std::mutex> lock(partitionCounterMutex);
    NES_INFO("PartitionManager: Registering " << partition.toString());

    //check if partition is present
    if (isRegistered(partition)) {
        // partition is contained
        partitionCounter[partition] = partitionCounter[partition] + 1;
    } else {
        partitionCounter[partition] = 0;
    }
    return partitionCounter[partition];
}

uint64_t PartitionManager::unregisterSubpartition(NesPartition partition) {
    unique_lock<std::mutex> lock(partitionCounterMutex);
    NES_INFO("PartitionManager: Unregistering " << partition.toString());

    // if partition is contained, decrement counter
    auto newCounter = partitionCounter.at(partition) - 1;

    if (newCounter == 0) {
        //if counter reaches 0, erase partition
        partitionCounter.erase(partition);
        return 0;
    }
    else {
        // else set and return new counter
        partitionCounter[partition] = newCounter;
        return newCounter;
    }
}

uint64_t PartitionManager::getSubpartitionCounter(NesPartition partition) {
    unique_lock<std::mutex> lock(partitionCounterMutex);
    return partitionCounter.at(partition);
}

uint64_t PartitionManager::deletePartition(NesPartition partition) {
    unique_lock<std::mutex> lock(partitionCounterMutex);
    NES_INFO("PartitionManager: Deleting " << partition.toString());
    return partitionCounter.erase(partition);
}

void PartitionManager::clear() {
    unique_lock<std::mutex> lock(partitionCounterMutex);
    NES_INFO("PartitionManager: Clearing registered partitions");
    partitionCounter.clear();
}

bool PartitionManager::isRegistered(NesPartition partition) const {
    //check if partition is present
    return partitionCounter.find(partition) != partitionCounter.end();
}

}