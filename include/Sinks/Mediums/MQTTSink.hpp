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


namespace NES {

class MQTTSink : public SinkMedium {

  public:
    //TODO: remove internal flag once the new network stack is in place
    MQTTSink(SinkFormatPtr sink_format, QuerySubPlanId parentPlanId, std::string& host,
             uint16_t port, std::string& user, std::string& password);
    ~MQTTSink();

    bool writeData(NodeEngine::TupleBuffer& input_buffer, NodeEngine::WorkerContextRef) override;
    void setup() override { connect(); };
    void shutdown() override{};
    const std::string toString() const override;

    /**
     * @brief Get MQTT sink port
     */
    int getPort();

    /**
     * @brief Get Zmq host name
     */
    const std::string getHost() const;

    /**
     * @brief Get Sink type
     */
    std::string toString() override;

    /**
    * @brief method to return the type of medium
    * @return type of medium
    */
    SinkMediumTypes getSinkMediumType();

  private:
    MQTTSink();

    QuerySubPlanId parentPlanId;

    std::string& host;
    uint16_t port;

    std::string& user;
    std::string& password;

    bool connected;

    bool connect();
    bool disconnect();
};
//TODO change to MQTT specific
typedef std::shared_ptr<MQTTSink> ZmqSinkPtr;
}// namespace NES

#endif// MQTTSINK_HPP
