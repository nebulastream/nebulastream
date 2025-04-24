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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_NETWORK_NETWORKSINKDESCRIPTOR_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_NETWORK_NETWORKSINKDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Network/NesPartition.hpp>
#include <Operators/LogicalOperators/Network/NodeLocation.hpp>
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <chrono>
#include <string>
#include <Util/CheckpointStorageType.hpp>
#include <Util/FaultToleranceType.hpp>

namespace NES::Network {

/**
 * @brief Descriptor defining properties used for creating physical zmq sink
 */
class NetworkSinkDescriptor : public SinkDescriptor {

  public:
    /**
     * @brief The constructor for the network sink descriptor
     * @param nodeLocation
     * @param nesPartition
     * @param waitTime
     * @param retryTimes
     * @param version The version number of the sink when it is started
     * @return SinkDescriptorPtr
     */
    static SinkDescriptorPtr create(const NodeLocation& nodeLocation,
                                    const NesPartition& nesPartition,
                                    std::chrono::milliseconds waitTime,
                                    uint32_t retryTimes,
                                    DecomposedQueryPlanVersion version,
                                    uint64_t numberOfOrigins,
                                    OperatorId uniqueId,
                                    FaultToleranceType faultTolerance = FaultToleranceType::NONE,
                                    CheckpointStorageType checkpointStorage = CheckpointStorageType::NONE);

    /**
     * @brief returns the string representation of the network sink
     * @return the string representation
     */
    std::string toString() const override;

    /**
     * @brief equal method for the NetworkSinkDescriptor
     * @param other
     * @return true if equal, else false
     */
    [[nodiscard]] bool equal(SinkDescriptorPtr const& other) override;

    /**
     * @brief getter for the node location
     * @return the node location
     */
    NodeLocation getNodeLocation() const;

    /**
     * @brief getter for the nes partition
     * @return the nes partition
     */
    NesPartition getNesPartition() const;

    /**
     * @brief getter for the wait time
     * @return the wait time
     */
    std::chrono::milliseconds getWaitTime() const;

    /**
     * @brief getter for the retry times
     * @return the retry times
     */
    uint8_t getRetryTimes() const;

    /**
     * @brief getter for fault tolerance
     * @return fault tolerance
     */
    FaultToleranceType getFaultTolerance() const;

    /**
     * @brief getter for storage option if checkpointing enabled
     * @return storage option
     */
    CheckpointStorageType getCheckpointStorage() const;

    /**
     * @brief getter for the sinks version
     * @return the version
     */
    uint16_t getVersion() const;

    /**
     * @brief getter for the network sinks unique id
     * @return the unique id
     */
    OperatorId getUniqueId() const;

  private:
    explicit NetworkSinkDescriptor(const NodeLocation& nodeLocation,
                                   const NesPartition& nesPartition,
                                   std::chrono::milliseconds waitTime,
                                   uint32_t retryTimes,
                                   DecomposedQueryPlanVersion version,
                                   uint64_t numberOfOrigins,
                                   OperatorId uniqueId,
                                   FaultToleranceType faultTolerance,
                                   CheckpointStorageType checkpointStorage);

    NodeLocation nodeLocation;
    NesPartition nesPartition;
    std::chrono::milliseconds waitTime;
    uint32_t retryTimes;
    DecomposedQueryPlanVersion version;
    OperatorId uniqueNetworkSinkId;
    FaultToleranceType faultTolerance;
    CheckpointStorageType checkpointStorage;
};

using NetworkSinkDescriptorPtr = std::shared_ptr<NetworkSinkDescriptor>;

}// namespace NES::Network

#endif// NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_NETWORK_NETWORKSINKDESCRIPTOR_HPP_
