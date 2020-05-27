
#include <Network/PartitionManager.hpp>
#include <Util/Logger.hpp>

namespace NES::Network {

uint64_t PartitionManager::registerSubpartition(NesPartition partition) {
    std::unique_lock<std::mutex> lock(partitionCounterMutex);
    //check if partition is present
    if (isRegistered(partition)) {
        // partition is contained
        partitionCounter[partition] = partitionCounter[partition] + 1;
    } else {
        partitionCounter[partition] = 0;
    }
    NES_INFO("PartitionManager: Registering " << partition.toString() << "<<" << partitionCounter[partition]);
    return partitionCounter[partition];
}

uint64_t PartitionManager::unregisterSubpartition(NesPartition partition) {
    std::unique_lock<std::mutex> lock(partitionCounterMutex);

    // if partition is contained, decrement counter
    auto counter = partitionCounter.at(partition);

    if (counter == 0) {
        //if counter reaches 0, log error
        NES_INFO("PartitionManager: Deleting " << partition.toString() << ", counter is at 0.");
        partitionCounter.erase(partition);
    } else {
        //else decrement counter
        partitionCounter[partition] = --counter;
        NES_INFO("PartitionManager: Unregistering " << partition.toString() << "; newCnt(" << counter << ")");
    }

    // return new counter
    return counter;
}

uint64_t PartitionManager::getSubpartitionCounter(NesPartition partition) {
    std::unique_lock<std::mutex> lock(partitionCounterMutex);
    return partitionCounter.at(partition);
}

uint64_t PartitionManager::deletePartition(NesPartition partition) {
    std::unique_lock<std::mutex> lock(partitionCounterMutex);
    NES_INFO("PartitionManager: Deleting " << partition.toString());
    return partitionCounter.erase(partition);
}

void PartitionManager::clear() {
    std::unique_lock<std::mutex> lock(partitionCounterMutex);
    NES_INFO("PartitionManager: Clearing registered partitions");
    partitionCounter.clear();
}

bool PartitionManager::isRegistered(NesPartition partition) const {
    //check if partition is present
    return partitionCounter.find(partition) != partitionCounter.end();
}

}// namespace NES::Network