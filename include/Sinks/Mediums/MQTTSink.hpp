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

#ifndef MQTTSINK_HPP
#define MQTTSINK_HPP

#ifdef ENABLE_MQTT_BUILD
#include <Operators/LogicalOperators/Sinks/MQTTSinkDescriptor.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/MQTTClientWrapper.hpp>
#include <cstdint>
#include <memory>
#include <string>

namespace NES {
/**
 * @brief Defining properties used for creating physical MQTT sink
 */
class MQTTSink : public SinkMedium {
  public:
    /**
     * @brief Creates the MQTT sink
     * @param address: address name of MQTT broker
     * @param clientId: client ID for MQTT client. If non is given, the operatorID is used automatically (see 'ConvertLogicalToPhysicalSink.cpp).
     * @param topic: MQTT topic chosen to publish client data to
     * @param user: user identification for client
     * @param maxBufferedMSGs: maximal number of messages that can be buffered by the client before disconnecting
     * @param timeUnit: time unit chosen by client user for message delay
     * @param messageDelay: time before next message is sent by client to broker
     * @param qualityOfService: either 'at most once' or 'at least once'
     * @param asynchronousClient: determine whether client is async- or synchronous
     * @return MQTT sink
     */
    // TODO change MSGS to Messages
    explicit MQTTSink(SinkFormatPtr sinkFormat,
                      QuerySubPlanId parentPlanId,
                      std::string const& address,
                      std::string const& clientId,
                      std::string const& topic,
                      std::string const& user,
                      uint64_t maxBufferedMSGs,
                      MQTTSinkDescriptor::TimeUnits timeUnit,
                      uint64_t messageDelay,
                      MQTTSinkDescriptor::ServiceQualities qualityOfService,
                      bool asynchronousClient);
    ~MQTTSink() NES_NOEXCEPT(false) override;

    bool writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) override;
    void setup() override { connect(); };
    void shutdown() override{};

    /**
     * @brief connect to a MQTT broker
     * @return true, if connect() was successful
     */
    bool connect();

    /**
     * @brief disconnect from a MQTT broker
     * @return true, if disconnect() was successful
     */
    bool disconnect();

    /**
     * @brief get address information from a MQTT sink client
     * @return address of MQTT broker
     */
    std::string getAddress() const;

    /**
     * @brief get clientId information from a MQTT sink client
     * @return id used by client
     */
    std::string getClientId() const;

    /**
     * @brief get topic information from a MQTT sink client
     * @return topic to which MQTT client sends messages
     */
    std::string getTopic() const;

    /**
     * @brief get user name for a MQTT sink client
     * @return user name used by MQTT client
     */
    std::string getUser() const;

    /**
     * @brief get the number of MSGs that can maximally be buffered (default is 60)
     * @return number of messages that can maximally be buffered
     */
    uint64_t getMaxBufferedMSGs() const;

    /**
     * @brief get the user chosen time unit (default is milliseconds)
     * @return time unit chosen for the message delay
     */
    MQTTSinkDescriptor::TimeUnits getTimeUnit() const;

    /**
     * @brief get the user chosen delay between two sent messages (default is 500)
     * @return length of the message delay
     */
    uint64_t getMsgDelay() const;

    /**
     * @brief get the value for the current quality of service
     * @return quality of service value
     */
    MQTTSinkDescriptor::ServiceQualities getQualityOfService() const;

    /**
     * @brief get bool that indicates whether the client is asynchronous or synchronous (default is true)
     * @return true if client is asynchronous, else false
     */
    bool getAsynchronousClient() const;

    /**
     * @brief Print MQTT Sink (schema, address, port, clientId, topic, user)
     */
    std::string toString() const override;

    /**
    * @brief method to return the type of medium
    * @return type of medium
    */
    SinkMediumTypes getSinkMediumType() override;

  private:
    [[maybe_unused]] QuerySubPlanId parentPlanId{};
    std::string address;
    std::string clientId;
    std::string topic;
    std::string user;
    uint64_t maxBufferedMSGs;
    MQTTSinkDescriptor::TimeUnits timeUnit;
    uint64_t messageDelay;
    MQTTSinkDescriptor::ServiceQualities qualityOfService;
    bool asynchronousClient;
    bool connected;
    std::chrono::duration<int64_t, std::ratio<1, 1000000000>> minDelayBetweenSends{};

    MQTTClientWrapperPtr client;
};
using MQTTSinkPtr = std::shared_ptr<MQTTSink>;

}// namespace NES
#endif
#endif//MQTTSINK
