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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_NETWORK_NETWORKSOURCEDESCRIPTOR_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_NETWORK_NETWORKSOURCEDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Network/NesPartition.hpp>
#include <Operators/LogicalOperators/Network/NodeLocation.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <chrono>
#include <string>

namespace NES::Network {

/**
 * @brief Descriptor defining properties used for creating physical zmq source inside the network stack
 */
class NetworkSourceDescriptor : public SourceDescriptor {

  public:
    /**
     * @brief The constructor for the network source descriptor
     * @param schema
     * @param nesPartition
     * @param nodeLocation
     * @param initialVersion the initial version of the source
     * @param uniqueNetworkSourceIdentifier a unique identifier for the source
     * @return instance of network source descriptor
     */
    static SourceDescriptorPtr create(SchemaPtr schema,
                                      const NesPartition& nesPartition,
                                      const NodeLocation& nodeLocation,
                                      std::chrono::milliseconds waitTime,
                                      uint32_t retryTimes,
                                      DecomposedQueryPlanVersion version,
                                      OperatorId uniqueNetworkSourceId);

    /**
     * @brief equal method for the NetworkSourceDescriptor
     * @param other
     * @return true if equal, else false
     */
    [[nodiscard]] bool equal(SourceDescriptorPtr const& other) const override;

    /**
     * @brief creates a string representation of the NetworkSourceDescriptor
     * @return the string representation
     */
    std::string toString() const override;

    /**
     * @brief getter for the nes partition
     * @return the nesPartition
     */
    NesPartition getNesPartition() const;

    NodeLocation getNodeLocation() const;

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
     * @brief getter for the version
     * @return the version
     */
    uint16_t getVersion() const;

    /**
     * @brief mark the source for reuse
     */
    void markForReuse();

    /**
     * @brief check if the source is marked for reuse and clear the mark
     * @return true if marked for reuse, else false
     */
    bool isMarkedForReuse();

    OperatorId getUniqueId() const;

    SourceDescriptorPtr copy() override;

  private:
    explicit NetworkSourceDescriptor(SchemaPtr schema,
                                     const NesPartition& nesPartition,
                                     const NodeLocation& nodeLocation,
                                     std::chrono::milliseconds waitTime,
                                     uint32_t retryTimes,
                                     DecomposedQueryPlanVersion version,
                                     OperatorId uniqueNetworkSourceId);

    NesPartition nesPartition;
    NodeLocation nodeLocation;
    std::chrono::milliseconds waitTime;
    uint8_t retryTimes;
    DecomposedQueryPlanVersion version;
    OperatorId uniqueNetworkSourceId;
    bool markedForReuse = false;
};

using NetworkSourceDescriptorPtr = std::shared_ptr<NetworkSourceDescriptor>;

}// namespace NES::Network

#endif// NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_NETWORK_NETWORKSOURCEDESCRIPTOR_HPP_
