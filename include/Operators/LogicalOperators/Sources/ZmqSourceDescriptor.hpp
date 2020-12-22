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

#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_ZMQSOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_ZMQSOURCEDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical zmq source
 */
class ZmqSourceDescriptor : public SourceDescriptor {

  public:
    static SourceDescriptorPtr create(SchemaPtr schema, std::string host, uint16_t port);
    static SourceDescriptorPtr create(SchemaPtr schema, std::string streamName, std::string host, uint16_t port);

    /**
     * @brief Get zmq host name
     */
    const std::string& getHost() const;

    /**
     * @brief Get zmq port number
     */
    uint16_t getPort() const;

    /**
     * Set the zmq port information
     * @param port : zmq port number
     */
    void setPort(uint16_t port);

    bool equal(SourceDescriptorPtr other) override;
    std::string toString() override;

  private:
    explicit ZmqSourceDescriptor(SchemaPtr schema, std::string host, uint16_t port);
    explicit ZmqSourceDescriptor(SchemaPtr schema, std::string streamName, std::string host, uint16_t port);

    std::string host;
    uint16_t port;
};

typedef std::shared_ptr<ZmqSourceDescriptor> ZmqSourceDescriptorPtr;

}// namespace NES

#endif//NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_ZMQSOURCEDESCRIPTOR_HPP_
