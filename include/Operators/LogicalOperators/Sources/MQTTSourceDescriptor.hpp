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
    enum TimeUnits { nanoseconds, milliseconds, seconds };
    enum DataType {JSON};
    /**
     * @brief create a source descriptor pointer for MQTT source
     * @param schema the schema of the data
     * @param serverAddress the server address to connect to (port included)
     * @param clientId identifies the client connecting to the server, each server has aunique clientID
     * @param user to connect to server
     * @param topic to subscribe to
     * @param timeUnits unit for the timed delay, default = nanoseconds
     * @param dataType data type that is send by the broker, default = JSON
     * @param messageDelay delay units for the messages, default = 0
     * @return source descriptor pointer to mqtt source
     */
    static SourceDescriptorPtr
    create(SchemaPtr schema, std::string serverAddress, std::string clientId, std::string user, std::string topic, DataType dataType, TimeUnits timeUnits, uint64_t messageDelay);
    /**
     * @brief create a source descriptor pointer for MQTT source
     * @param logicalStreamName Name of the logical data stream
     * @param schema the schema of the data
     * @param serverAddress the server address to connect to (port included)
     * @param clientId identifies the client connecting to the server, each server has aunique clientID
     * @param user to connect to server
     * @param topic to subscribe to
     * @param timeUnits unit for the timed delay, default = nanoseconds
     * @param dataType data type that is send by the broker, default = JSON
     * @param messageDelay delay units for the messages, default = 0
     * @return source descriptor pointer to mqtt source
     */
    static SourceDescriptorPtr create(SchemaPtr schema,
                                      std::string logicalStreamName,
                                      std::string serverAddress,
                                      std::string clientId,
                                      std::string user,
                                      std::string topic,
                                      DataType dataType,
                                      TimeUnits timeUnits,
                                      uint64_t messageDelay);

    /**
     * @brief get MQTT server address
     * @return serverAddress
     */
    std::string getServerAddress() const;

    /**
     * @brief get desired mqtt server client id
     * @return clientId
     */
    std::string getClientId() const;

    /**
     * @brief get user name
     * @return user
     */
    std::string getUser() const;

    /**
     * @brief getter for topic
     * @return topic
     */
    std::string getTopic() const;

    /**
     * @brief getter for dataType
     * @return dataType
     */
    MQTTSourceDescriptor::DataType getDataType() const;
    /**
     * @brief getter for timeUnit
     * @return timeUnit
     */
    TimeUnits getTimeUnit() const;
    /**
     * @brief getter for messageDelay
     * @return messageDelay
     */
    uint64_t getMessageDelay() const;

    /**
     * checks if two mqtt source descriptors are the same
     * @param other
     * @return true if they are the same
     */
    [[nodiscard]] bool equal(SourceDescriptorPtr const& other) override;

    std::string toString() override;

  private:
    /**
     * @brief mqtt source descriptor constructor
     * @param schema the schema of the data
     * @param serverAddress the server address to connect to (port included)
     * @param clientId unique server id
     * @param user to connect to server
     * @param topic to subscribe to
     * @param timeUnits unit for the timed delay, default = nanoseconds
     * @param dataType data type that is send by the broker, default = JSON
     * @param messageDelay delay units for the messages, default = 0
     */
    explicit MQTTSourceDescriptor(SchemaPtr schema,
                                  std::string serverAddress,
                                  std::string clientId,
                                  std::string user,
                                  std::string topic,
                                  DataType dataType,
                                  TimeUnits timeUnits,
                                  uint64_t messageDelay);
    /**
     * @brief mqtt source descriptor constructor
     * @param schema the schema of the data
     * @param logicalStreamName name of the data stream
     * @param serverAddress the server address to connect to (port included)
     * @param clientId unique server id
     * @param user to connect to server
     * @param topic to subscribe to
     * @param timeUnits unit for the timed delay, default = nanoseconds
     * @param dataType data type that is send by the broker, default = JSON
     * @param messageDelay delay units for the messages, default = 0
     */
    explicit MQTTSourceDescriptor(SchemaPtr schema,
                                  std::string logicalStreamName,
                                  std::string serverAddress,
                                  std::string clientId,
                                  std::string user,
                                  std::string topic,
                                  DataType dataType,
                                  TimeUnits timeUnits,
                                  uint64_t messageDelay);

    std::string serverAddress;
    std::string clientId;
    std::string user;
    std::string password;
    std::string topic;
    DataType dataType;
    TimeUnits timeUnits;
    uint64_t messageDelay;
};

using MQTTSourceDescriptorPtr = std::shared_ptr<MQTTSourceDescriptor>;

}// namespace NES

#endif
#endif//NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_MQTTSOURCEDESCRIPTOR_HPP_