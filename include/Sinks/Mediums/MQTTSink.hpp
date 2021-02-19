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
    enum TimeUnits { nanoseconds, milliseconds, seconds };
    enum ServiceQualities { atMostOnce, atLeastOnce };// MQTT also offers exactly once, add if needed
    /**
     * @brief Creates the MQTT sink
     * @param address: address name of MQTT broker
     * @param clientId: client ID for MQTT client
     * @param topic: MQTT topic chosen to publish client data to
     * @param user: user identification for client
     * @param maxBufferedMSGs: maximal number of messages that can be buffered by the client before disconnecting
     * @param timeUnit: time unit chosen by client user for message delay
     * @param msgDelay: time before next message is sent by client to broker
     * @param qualityOfService: either 'at most once' or 'at least once'
     * @param asynchronousClient: determine whether client is async- or synchronous
     * @return MQTT sink
     */
    MQTTSink(SinkFormatPtr sinkFormat, QuerySubPlanId parentPlanId, const std::string address, const std::string clientId,
             const std::string topic, const std::string user, uint64_t maxBufferedMSGs, const TimeUnits timeUnit,
             uint64_t msgDelay, const ServiceQualities qualityOfService, bool asynchronousClient);
    ~MQTTSink();

    bool writeData(NodeEngine::TupleBuffer& input_buffer, NodeEngine::WorkerContextRef) override;
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
    const std::string getAddress() const;

    /**
     * @brief get clientId information from a MQTT sink client
     * @return id used by client
     */
    const std::string getClientId() const;

    /**
     * @brief get topic information from a MQTT sink client
     * @return topic to which MQTT client sends messages
     */
    const std::string getTopic() const;

    /**
     * @brief get user name for a MQTT sink client
     * @return user name used by MQTT client
     */
    const std::string getUser() const;

    /**
     * @brief get the number of MSGs that can maximally be buffered (default is 60)
     * @return number of messages that can maximally be buffered
     */
    uint64_t getMaxBufferedMSGs();

    /**
     * @brief get the user chosen time unit (default is milliseconds)
     * @return time unit chosen for the message delay
     */
    const TimeUnits getTimeUnit() const;

    /**
     * @brief get the user chosen delay between two sent messages (default is 500)
     * @return length of the message delay
     */
    uint64_t getMsgDelay();

    /**
     * @brief get the value for the current quality of service
     * @return quality of service value
     */
    const ServiceQualities getQualityOfService() const;

    /**
     * @brief get bool that indicates whether the client is asynchronous or synchronous (default is true)
     * @return true if client is asynchronous, else false
     */
    bool getAsynchronousClient();

    /**
     * @brief Print MQTT Sink (schema, address, port, clientId, topic, user)
     */
    const std::string toString() const override;

    /**
    * @brief method to return the type of medium
    * @return type of medium
    */
    SinkMediumTypes getSinkMediumType() override;

  private:
    QuerySubPlanId parentPlanId;
    std::string address;
    std::string clientId;
    std::string topic;
    std::string user;
    uint64_t maxBufferedMSGs;
    TimeUnits timeUnit;
    uint64_t msgDelay;
    ServiceQualities qualityOfService;
    bool asynchronousClient;
    bool connected;
    std::chrono::duration<int64_t, std::ratio<1, 1000000000>> minDelayBetweenSends;

    MQTTClientWrapperPtr client;
};
typedef std::shared_ptr<MQTTSink> MQTTSinkPtr;

}// namespace NES

#endif
#endif//MQTTSINK
