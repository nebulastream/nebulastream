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

#ifndef NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_SINKS_NETWORK_SINK_DESCRIPTOR_HPP_
#define NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_SINKS_NETWORK_SINK_DESCRIPTOR_HPP_

#include <Network/NesPartition.hpp>
#include <Network/NodeLocation.hpp>
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Util/UtilityFunctions.hpp>
#include <chrono>
#include <string>
namespace NES {
namespace Network {

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
     * @return SinkDescriptorPtr
     */
    static SinkDescriptorPtr
    create(NodeLocation nodeLocation, NesPartition nesPartition, std::chrono::milliseconds waitTime, uint32_t retryTimes, OperatorId operatorId);

    /**
     * @brief returns the string representation of the network sink
     * @return the string representation
     */
    std::string toString() override;

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
     * @brief getter for global network sink id
     * @return the global operator id
     */
    OperatorId getOperatorId();

  private:
    explicit NetworkSinkDescriptor(NodeLocation nodeLocation,
                                   NesPartition nesPartition,
                                   std::chrono::milliseconds waitTime,
                                   uint32_t retryTimes,
                                   OperatorId operatorId = Util::getNextOperatorId());

    NodeLocation nodeLocation;
    NesPartition nesPartition;
    std::chrono::milliseconds waitTime;
    uint32_t retryTimes;
    OperatorId operatorId;
};

using NetworkSinkDescriptorPtr = std::shared_ptr<NetworkSinkDescriptor>;

}// namespace Network
}// namespace NES

#endif// NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_SINKS_NETWORK_SINK_DESCRIPTOR_HPP_
