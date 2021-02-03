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

#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_MQTTSOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_MQTTSOURCEDESCRIPTOR_HPP_

#ifdef ENABLE_OPC_BUILD

#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <mqtt/async_client.h>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical opc source
 */
class MQTTSourceDescriptor : public SourceDescriptor {

  public:
    static SourceDescriptorPtr create(SchemaPtr schema, std::string serverAddress, std::string clientId,
                                      std::string user, std::string password, std::string topic);

    static SourceDescriptorPtr create(SchemaPtr schema, std::string streamName, std::string serverAddress, std::string clientId,
                                      std::string user, std::string password, std::string topic);

    /**
     * @brief get OPC server url
     */
    const std::string getServerAddress() const;

    /**
     * @brief get desired node id
     */
    const std::string getClientId() const;

    /**
     * @brief get user name
     */
    const std::string getUser() const;

    /**
     * @brief get password
     */
    const std::string getPassword() const;

    const std::string getTopic() const;

    bool equal(SourceDescriptorPtr other) override;

    std::string toString() override;

  private:
    explicit MQTTSourceDescriptor(SchemaPtr schema, const std::string serverAddress, const std::string clientId,
                                  const std::string user, const std::string password,
                                  const std::string topic);

    explicit MQTTSourceDescriptor(SchemaPtr schema, std::string streamName, const std::string serverAddress,
                                  const std::string clientId, const std::string user, const std::string password,
                                  const std::string topic);

    std::string serverAddress;
    std::string clientId;
    std::string user;
    std::string password;
    std::string topic;
};

typedef std::shared_ptr<MQTTSourceDescriptor> MQTTSourceDescriptorPtr;

}// namespace NES

#endif
#endif//NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_MQTTSOURCEDESCRIPTOR_HPP_