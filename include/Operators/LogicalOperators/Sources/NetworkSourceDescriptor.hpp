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

#ifndef NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_SOURCES_NETWORK_SOURCE_DESCRIPTOR_HPP_
#define NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_SOURCES_NETWORK_SOURCE_DESCRIPTOR_HPP_

#include <Network/NesPartition.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <string>

namespace NES {
namespace Network {

/**
 * @brief Descriptor defining properties used for creating physical zmq source inside the network stack
 */
class NetworkSourceDescriptor : public SourceDescriptor {

  public:
    /**
     * @brief The constructor for the network source descriptor
     * @param schema
     * @param nesPartition
     * @return instance of network source descriptor
     */
    static SourceDescriptorPtr create(SchemaPtr schema, NesPartition nesPartition, uint64_t sinkTopologyNodeId = 0);

    /**
     * @brief equal method for the NetworkSourceDescriptor
     * @param other
     * @return true if equal, else false
     */
    [[nodiscard]] bool equal(SourceDescriptorPtr const& other) override;

    /**
     * @brief creates a string representation of the NetworkSourceDescriptor
     * @return the string representation
     */
    std::string toString() override;

    /**
     * @brief getter for the nes partition
     * @return the nesPartition
     */
    NesPartition getNesPartition() const;

    /**
     *
     * @return the topology node from which this source recieves data
     */
    uint64_t getSinkTopologyNode();

  private:
    explicit NetworkSourceDescriptor(SchemaPtr schema, NesPartition nesPartition, uint64_t sinkTopologyNodeId);

    NesPartition nesPartition;

    uint64_t sinkTopologyNodeId;
};

using networkSourceDescriptorPtr = std::shared_ptr<NetworkSourceDescriptor>;

}// namespace Network
}// namespace NES

#endif// NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_SOURCES_NETWORK_SOURCE_DESCRIPTOR_HPP_
