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

#ifdef ENABLE_MQTT_BUILD
#include <cstdint>
#include <memory>
#include <string>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <mqtt/client.h>
#include <Util/MQTTClientWrapper.hpp>

namespace NES {
/**
 * @brief Defining properties used for creating physical MQTT sink
 */
class MQTTSink : public SinkMedium {
  public:
    /**
     * @brief Creates the MQTT sink
     * @param address: address name of MQTT broker
     * @param clientId: client ID for MQTT client
     * @param topic: MQTT topic chosen to publish client data to
     * @param user: user identification for client
     * @param maxBufferedMSGs: maximal number of messages that can be buffered by the client before disconnecting
     * @param timeUnit: time unit chosen by client user for message delay
     * @param msgDelay: time before next message is sent by client to broker
     * @param asynchronousClient: determine whether client is async- or synchronous
     * @return MQTT sink
     */
    MQTTSink(SinkFormatPtr sinkFormat, QuerySubPlanId parentPlanId, const std::string& address,
             const std::string& clientId, const std::string& topic, const std::string& user,
             const uint32_t maxBufferedMSGs = 60, char timeUnit = 'm', uint64_t msgDelay = 500, bool asynchronousClient = 1);
    ~MQTTSink();

    bool writeData(NodeEngine::TupleBuffer& input_buffer, NodeEngine::WorkerContextRef) override;
    void setup() override { connect(); };
    void shutdown() override{};
    bool connect();
    bool disconnect();

    /**
     * @brief get address information from a MQTT sink client
     */
    const std::string& getAddress() const;

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

    /**
     * @brief Get connected status of MQTT sink
     */
    bool getConnected();

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
    uint32_t maxBufferedMSGs;
    uint8_t timeUnit; //'n'-nanoseconds, 'm'-milliseconds, 's'-seconds
    uint64_t msgDelay;
    bool asynchronousClient;
    char qualityOfService; // 0-at most once, 1-at least once, 2-exactly once
    bool connected;
    std::chrono::duration<int64_t, std::ratio<1, 1000000000>> sendDuration;


    MQTTClientWrapper client;
};
typedef std::shared_ptr<MQTTSink> MQTTSinkPtr;


}// namespace NES


#endif//
