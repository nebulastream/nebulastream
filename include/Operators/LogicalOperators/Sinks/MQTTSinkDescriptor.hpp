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
 * @brief Descriptor defining properties used for creating physical mqtt sink
 */
class MQTTSinkDescriptor : public SinkDescriptor {

  public:
    /**
     * @brief Creates the MQTT sink description
     * @param host: host name of MQTT broker
     * @param port: port of MQTT broker
     * @param clientId: client ID for MQTT client
     * @param topic: MQTT topic chosen to publish client data to
     * @param user: user identification for client
     * @param maxBufferedMSGs: maximal number of messages that can be buffered by the client before disconnecting
     * @param timeUnit: time unit chosen by client user for message delay
     * @param msgDelay: time before next message is sent by client to broker
     * @param asynchronousClient: determine whether client is async- or synchronous
     * @return descriptor for MQTT sink
     */
    static SinkDescriptorPtr create( const std::string& host, const uint16_t port, const std::string& clientId,
                                     const std::string& topic, const std::string& user, const uint32_t maxBufferedMSGs,
                                     const char timeUnit, const uint64_t msgDelay, const bool asynchronousClient);

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

    /**
     * @brief get the number of MSGs that can maximally be buffered (default is 60)
     */
    const uint32_t getMaxBufferedMSGs() const;

    /**
     * @brief get the user chosen time unit (default is milliseconds)
     */
    const char getTimeUnit() const;


    /**
     * @brief get the user chosen delay between two sent messages (default is 500)
     */
    const uint64_t getMsgDelay() const;

    /**
     * @brief get bool that indicates whether the client is asynchronous or synchronous (default is true)
     */
    const bool getAsynchronousClient() const;

    void setPort(uint16_t);
    void setMaxBufferedMSGs(uint32_t);
    void setTimeUnit(char);
    void setMsgDelay(uint64_t);

    std::string toString() override;
    bool equal(SinkDescriptorPtr other) override;

  private:
    explicit MQTTSinkDescriptor(const std::string& host, const uint16_t port, const std::string& clientId, const std::string& topic,
                       const std::string& user, const uint32_t maxBufferedMSGs, const char timeUnit, const uint64_t msgDelay,
                       const bool asynchronousClient);

    std::string host;
    uint16_t port;

    std::string clientId;
    std::string topic;

    std::string user;

    uint32_t maxBufferedMSGs;
    uint8_t timeUnit; //'n'-nanoseconds, 'm'-milliseconds, 's'-seconds

    uint64_t msgDelay;
    bool asynchronousClient;
};

typedef std::shared_ptr<MQTTSinkDescriptor> MQTTSinkDescriptorPtr;

}// namespace NES
#endif

#endif//NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_MQTTSINKDESCRIPTOR_HPP_
