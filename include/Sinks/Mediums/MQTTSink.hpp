//
// Created by rudi on 01.02.21.
//

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

#include <cstdint>
#include <memory>
#include <string>
#include <zmq.hpp>

#include <Sinks/Mediums/SinkMedium.hpp>
//#include "mqtt/client.h"
#include "mqtt/async_client.h"


namespace NES {

class MQTTSink : public SinkMedium {

  public:
    //TODO: remove internal flag once the new network stack is in place
    MQTTSink(SinkFormatPtr sinkFormat, QuerySubPlanId parentPlanId, const std::string& host,
             const uint16_t port, const std::string& clientId, const std::string& topic,
             const std::string& user);
    ~MQTTSink();

    bool writeData(NodeEngine::TupleBuffer& input_buffer, NodeEngine::WorkerContextRef) override;
    void setup() override { connect(); };
    void shutdown() override{};

    /**
     * @brief Get MQTT sink port
     */
    int getPort();

    /**
     * @brief Get MQTT host name
     */
    const std::string& getHost() const;

    /**
     * @brief Get MQTT topic
     */
    const std::string& getTopic() const;

    /**
     * @brief Get MQTT clientId
     */
    const std::string& getClientId() const;

    /**
     * @brief Get MQTT user
     */
    const std::string& getUser() const;

    /**
     * @brief Print MQTT Sink (schema, host, port, clientId, topic, user)
     */
    const std::string toString() const override;

    /**
    * @brief method to return the type of medium
    * @return type of medium
    */
    SinkMediumTypes getSinkMediumType();

  private:
    MQTTSink();

    QuerySubPlanId parentPlanId;

    std::string host;
    uint16_t port;

    std::string clientId;
    std::string topic;

    std::string user;

    uint8_t qualityOfService; // 0-at most once, 1-at least once, 2-exactly once
    uint32_t maxBufferedMSGs;

    uint64_t sendPeriod;
    uint8_t timeType; //0-nanoseconds, 1-milliseconds, 2-seconds

    std::string address;
    mqtt::async_client client;

// not needed yet, but possibly in the future:
//    std::string& password;
//    std::string& period;
//    std::string& persistDir;

    bool connected;

    bool connect();
    bool disconnect();
};
//TODO change to MQTT specific
typedef std::shared_ptr<MQTTSink> MQTTSinkPtr;
}// namespace NES

#endif// MQTTSINK_HPP
