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

#ifndef NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_MQTTSINKDESCRIPTOR_HPP_
#define NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_MQTTSINKDESCRIPTOR_HPP_

#ifdef ENABLE_MQTT_BUILD

#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical opc sink
 */
class MQTTSinkDescriptor : public SinkDescriptor {

  public:
    /**
     * @brief Creates the OPC sink description
     * @param url: server url used to connect to OPC server
     * @param nodeId: id of node to write data to
     * @param user: user name for server
     * @param password: password for server
     * @return descriptor for OPC sink
     */
    static SinkDescriptorPtr create(const std::string& host, uint16_t port, const std::string& clientId,
                                    const std::string& topic, const std::string& user);

    /**
     * @brief get host information from a MQTT sink client
     */
    const std::string& getHost() const;

    /**
     * @brief get port information from a MQTT sink client
     */
    uint16_t getPort() const;

    /**
     * @brief get clientId information from a MQTT sink client
     */
    const std::string& getClientId() const;

    /**
     * @brief get topic information from a MQTT sink client
     */
    const std::string& getTopic() const;

    /**
     * @brief get user name for a MQTT sink client
     */
    const std::string& getUser() const;

    void setPort(uint16_t);

    std::string toString() override;
    bool equal(SinkDescriptorPtr other) override;

  private:
    explicit MQTTSinkDescriptor(const std::string& host, uint16_t port, const std::string& clientId,
                                const std::string& topic, const std::string& user);

    const std::string& host;
    uint16_t port;

    const std::string& clientId;
    const std::string& topic;

    const std::string& user;
};

typedef std::shared_ptr<MQTTSinkDescriptor> MQTTSinkDescriptorPtr;

}// namespace NES
#endif

#endif//NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_MQTTSINKDESCRIPTOR_HPP_
