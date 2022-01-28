/*
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

#ifndef NES_INCLUDE_NETWORK_PARTITIONMANAGER_HPP_
#define NES_INCLUDE_NETWORK_PARTITIONMANAGER_HPP_

#include <Network/NetworkForwardRefs.hpp>
#include <Network/PartitionRegistrationStatus.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

namespace NES::Network {

/**
 * @brief this class keeps track of all ready partitions (and their subpartitions)
 * It keeps track of the ref cnt for each partition and associated data emitter
 * A data emitter is notified once there is data for its partition.
 * Pinning a partition for a Consumer/Producer means increasing its reference counter to n:
 * Consumer: there are n "listeners" that are to consume the data of a partition
 * Producer: there are n "emitters" that are to produce the data for a partition
 * When the reference counter reaches 0, it means that none requires a partition.
 */
class PartitionManager {
  public:
  private:
    /**
     * @brief Helper class to store a partition's ref cnt and data emitter
     */
    class PartitionProducerEntry {
      public:
        /**
         * @brief Creates a new partition entry info with ref cnt = 0
         * @param emitter the data emitter that must be notified upon arrival of new data
         */
        explicit PartitionProducerEntry(NodeLocation&& receiverLocation);

        /**
         * @return the refcnt of the partition
         */
        [[nodiscard]] uint64_t count() const;

        /**
         * @brief increment ref cnt by 1
         */
        void pin();

        /**
         * @brief decrement ref cnt by 1
         */
        void unpin();

        /**
         * @brief Attach event listener for a partition
         */
        void registerEventListener(Runtime::RuntimeEventListenerPtr eventListener);

        /**
         * @brief Returns attached event listener for a partition
         * @return the attached event listener for a partition
         */
        Runtime::RuntimeEventListenerPtr getEventListener() const;

      private:
        uint64_t partitionCounter{1};
        NodeLocation receiverLocation;
        Runtime::RuntimeEventListenerPtr eventListener;
    };

    /**
     * @brief Helper class to store a partition's ref cnt and data emitter
     */
    class PartitionConsumerEntry {
      public:
        /**
         * @brief Creates a new partition entry info with ref cnt = 0
         * @param emitter the data emitter that must be notified upon arrival of new data
         */
        explicit PartitionConsumerEntry(NodeLocation&& senderLocation, DataEmitterPtr&& consumer);

        /**
         * @return the refcnt of the partition
         */
        [[nodiscard]] uint64_t count() const;

        /**
         * @brief increment ref cnt by 1
         */
        void pin();

        /**
         * @brief decrement ref cnt by 1
         */
        void unpin();

        /**
         * @return the data emitter
         */
        DataEmitterPtr getConsumer();

      private:
        uint64_t partitionCounter{1};
        NodeLocation senderLocation;
        DataEmitterPtr consumer{nullptr};
    };

  public:
    PartitionManager() = default;

    ~PartitionManager();

    /**
     * @brief Registers a subpartition in the PartitionManager. If the subpartition does not exist a new entry is
     * added in the partition table, otherwise the counter is incremented.
     * @param the partition
     * @emitter
     * @return true if this is  the first time the partition was registered, false otherwise
     */
    bool registerSubpartitionConsumer(NesPartition partition, NodeLocation nodeLocation, DataEmitterPtr emitter);

    /**
     * @brief Increment the subpartition counter
     * @param partition the partition
     */
    void pinSubpartitionConsumer(NesPartition partition);

    /**
     * @brief Unregisters a subpartition in the PartitionManager. If the subpartition does not exist or the current
     * counter is 0 an error is thrown.
     * @param the partition
     * @return true if the partition counter got to zero, false otherwise
     */
    bool unregisterSubpartitionConsumer(NesPartition partition);

    /**
     * @brief Returns the current counter of a given partition. Throws error if not existing.
     * @param the partition
     * @return the counter of the partition
     * @throw  std::out_of_range  If no such data is present.
     */
    std::optional<uint64_t> getSubpartitionConsumerCounter(NesPartition partition);

    /**
     * @brief checks if a partition is registered
     * @param the partition
     * @return a PartitionRegistrationStatus
     */
    PartitionRegistrationStatus getConsumerRegistrationStatus(NesPartition partition) const;

    /**
     * @brief Registers a subpartition in the PartitionManager. If the subpartition does not exist a new entry is
     * added in the partition table, otherwise the counter is incremented.
     * @param the partition
     * @emitter
     * @return true if this is  the first time the partition was registered, false otherwise
     */
    bool registerSubpartitionProducer(NesPartition partition, NodeLocation nodeLocation);

    /**
     * @brief Increment the subpartition counter
     * @param partition the partition
     */
    void pinSubpartitionProducer(NesPartition partition);

    /**
     * @brief Unregisters a subpartition in the PartitionManager. If the subpartition does not exist or the current
     * counter is 0 an error is thrown.
     * @param the partition
     * @return true if the partition counter got to zero, false otherwise
     */
    bool unregisterSubpartitionProducer(NesPartition partition);

    /**
     * @brief Returns the current counter of a given partition. Throws error if not existing.
     * @param the partition
     * @return the counter of the partition
     * @throw  std::out_of_range  If no such data is present.
     */
    std::optional<uint64_t> getSubpartitionProducerCounter(NesPartition partition);

    /**
     * @brief checks if a partition is registered
     * @param the partition
     * @return a PartitionRegistrationStatus
     */
    PartitionRegistrationStatus getProducerRegistrationStatus(NesPartition partition) const;

    /**
     * @brief Returns the data emitter of a partition
     * @param partition
     * @return the data emitter of a partition
     */
    DataEmitterPtr getDataEmitter(NesPartition partition);

    /**
     * @brief This method adds a new event listener for a partition
     * @param partition
     * @param eventListener
     * @return true if successful
     */
    bool addSubpartitionEventListener(NesPartition partition,
                                      NodeLocation nodeLocation,
                                      Runtime::RuntimeEventListenerPtr eventListener);

    /**
     * @brief Retrieve event listener for a partition
     * @param partition the partition to lookup
     * @return the event listener for a partition
     */
    Runtime::RuntimeEventListenerPtr getEventListener(NesPartition partition) const;

    /**
     * @brief clears all registered partitions
     */
    void clear();

  private:
    std::unordered_map<NesPartition, PartitionProducerEntry> producerPartitions;
    std::unordered_map<NesPartition, PartitionConsumerEntry> consumerPartitions;
    mutable std::recursive_mutex producerPartitionsMutex;
    mutable std::recursive_mutex consumerPartitionsMutex;
};
using PartitionManagerPtr = std::shared_ptr<PartitionManager>;
}// namespace NES::Network

#endif  // NES_INCLUDE_NETWORK_PARTITIONMANAGER_HPP_
