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

#ifdef ENABLE_MQTT_BUILD

#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical mqtt source
 */
class MQTTSourceDescriptor : public SourceDescriptor {

  public:
    /**
     * @brief create a source descriptor pointer for MQTT source
     * @param schema the schema of the data
     * @param serverAddress the server address to connect to (port included)
     * @param clientId identifies the client connecting to the server, each server has aunique clientID
     * @param user to connect to server
     * @param topic to subscribe to
     * @return source descriptor pointer to mqtt source
     */
    static SourceDescriptorPtr create(SchemaPtr schema, std::string serverAddress, std::string clientId, std::string user,
                                      std::string topic);
    /**
     * @brief create a source descriptor pointer for MQTT source
     * @param streamName Name of the logical data stream
     * @param schema the schema of the data
     * @param serverAddress the server address to connect to (port included)
     * @param clientId identifies the client connecting to the server, each server has aunique clientID
     * @param user to connect to server
     * @param topic to subscribe to
     * @return source descriptor pointer to mqtt source
     */
    static SourceDescriptorPtr create(SchemaPtr schema, std::string streamName, std::string serverAddress, std::string clientId,
                                      std::string user, std::string topic);

    /**
     * @brief get MQTT server address
     * @return serverAddress
     */
    const std::string getServerAddress() const;

    /**
     * @brief get desired mqtt server client id
     * @return clientId
     */
    const std::string getClientId() const;

    /**
     * @brief get user name
     * @return user
     */
    const std::string getUser() const;

    /**
     * @brief getter for topic
     * @return topic
     */
    const std::string getTopic() const;

    /**
     * checks if two mqtt source descriptors are the same
     * @param other
     * @return true if they are the same
     */
    bool equal(SourceDescriptorPtr other) override;

    std::string toString() override;

  private:
    /**
     * @brief mqtt source descriptor constructor
     * @param schema the schema of the data
     * @param serverAddress the server address to connect to (port included)
     * @param clientId unique server id
     * @param user to connect to server
     * @param topic to subscribe to
     */
    explicit MQTTSourceDescriptor(SchemaPtr schema, const std::string serverAddress, const std::string clientId,
                                  const std::string user, const std::string topic);
    /**
     * @brief mqtt source descriptor constructor
     * @param schema the schema of the data
     * @param streamName name of the data stream
     * @param serverAddress the server address to connect to (port included)
     * @param clientId unique server id
     * @param user to connect to server
     * @param topic to subscribe to
     */
    explicit MQTTSourceDescriptor(SchemaPtr schema, std::string streamName, const std::string serverAddress,
                                  const std::string clientId, const std::string user, const std::string topic);

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