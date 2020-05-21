#ifndef NES_PARTITIONMANAGER_HPP
#define NES_PARTITIONMANAGER_HPP

#include <Network/NetworkCommon.hpp>
#include <map>
#include <mutex>
#include <unordered_map>

namespace NES::Network {

class PartitionManager {
  public:
    /**
     * @brief Registers a subpartition in the PartitionManager. If the subpartition does not exist a new entry is
     * added in the partition table, otherwise the counter is incremented.
     * @param the partition
     * @return the new counter
     */
    uint64_t registerSubpartition(NesPartition partition);

    /**
     * @brief Unregisters a subpartition in the PartitionManager. If the subpartition does not exist or the current
     * counter is 0 an error is thrown.
     * @param the partition
     * @return the new counter
     * @throw  std::out_of_range  If no such data is present.
     */
    uint64_t unregisterSubpartition(NesPartition partition);

    /**
     * @brief Returns the current counter of a given partition. Throws error if not existing.
     * @param the partition
     * @return the counter of the partition
     * @throw  std::out_of_range  If no such data is present.
     */
    uint64_t getSubpartitionCounter(NesPartition partition);

    /**
     * @brief checks if a partition is registered
     * @param the partition
     * @return true if registered, else false
     */
    bool isRegistered(NesPartition partition) const;

    /**
     * @brief removes a subpartition completely from the entry list
     * @param the partition
     * @return  The number of elements erased.
     */
    uint64_t deletePartition(NesPartition partition);

    /**
     * @brief clears all registered partitions
     */
    void clear();

  private:
    std::unordered_map<NesPartition, uint64_t> partitionCounter;
    std::mutex partitionCounterMutex;
};
typedef std::shared_ptr<PartitionManager> PartitionManagerPtr;

}// namespace NES::Network

#endif//NES_PARTITIONMANAGER_HPP
