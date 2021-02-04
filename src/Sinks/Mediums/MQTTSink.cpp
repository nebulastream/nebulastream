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

#include <Sinks/Mediums/MQTTSink.hpp>

#include <cassert>
#include <cstdint>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>

#include <NodeEngine/QueryManager.hpp>
#include <Util/Logger.hpp>

//#include "mqtt/client.h"
#include "mqtt/async_client.h"

namespace NES {

SinkMediumTypes MQTTSink::getSinkMediumType() { return MQTT_SINK; }

MQTTSink::MQTTSink(SinkFormatPtr sinkFormat, QuerySubPlanId parentPlanId, const std::string& host,
                   const uint16_t port, const std::string& clientId, const std::string& topic,
                   const std::string& user)
    : SinkMedium(sinkFormat, parentPlanId), host(host), port(port), clientId(clientId),topic(topic), user(user),
                 qualityOfService(1), maxBufferedMSGs(60), sendPeriod(1),
                 timeType(2), address(std::string("tcp://") + host + std::string(":") + std::to_string(port)),
                 client(mqtt::async_client(address, clientId, maxBufferedMSGs)), connected(false){

    NES_DEBUG("MQTT Sink  " << this << ": Init MQTT Sink to " << host << ":" << port);
    std::cout << "ClientID Constructor test: " << this->getClientId() << '\n';
}

MQTTSink::~MQTTSink() {
    NES_DEBUG("MQTTSink::~MQTTSink: destructor called");
    bool success = disconnect();
    if (success) {
        NES_DEBUG("MQTTSink  " << this << ": Destroy MQTT Sink");
    } else {
        NES_ERROR("MQTTSink  " << this << ": Destroy MQTT Sink failed cause it could not be disconnected");
        throw Exception("MQTT Sink destruction failed");
    }
    NES_DEBUG("MQTTSink  " << this << ": Destroy MQTT Sink");
}

bool MQTTSink::writeData(NodeEngine::TupleBuffer& inputBuffer, NodeEngine::WorkerContextRef) {
    std::cout << inputBuffer.getBufferSize() << '\n';
    return false;
}

//TODO add more to print?
const std::string MQTTSink::toString() const {
    std::stringstream ss;
    ss << "MQTT_SINK(";
    ss << "SCHEMA("    << sinkFormat->getSchemaPtr()->toString() << "), ";
    ss << "HOST="      << host     << ", ";
    ss << "PORT="      << std::to_string(port)     << ", ";
    ss << "CLIENT_ID=" << clientId << ", ";
    ss << "TOPIC="     << topic    << ", ";
    ss << "USER="      << user     << ", ";
    ss << "QUALITY_OF_SERVICE=" << std::to_string(qualityOfService) << ", ";
    ss << "MAX_BUFFERED_MESSAGES=" << maxBufferedMSGs << ", ";
    ss << "SEND_PERIOD=" << sendPeriod << ", ";
    ss << "TIME_TYPE=" << std::to_string(timeType) << ", ";
    ss << "\")";
    return ss.str();
}

bool MQTTSink::connect() {
    if (!connected) {
        try {
            auto PERIOD = std::chrono::nanoseconds(sendPeriod) * ((timeType > 0) * 100000) * ((timeType > 1) * 1000);

            auto connOpts = mqtt::connect_options_builder()
                .keep_alive_interval(maxBufferedMSGs  * PERIOD)
                .clean_session(true)
                .automatic_reconnect(true)
                .finalize();

            // Create a topic object. This is a convenience since we will
            // repeatedly publish messages with the same parameters.
            mqtt::topic top(client, topic, qualityOfService, true);

            // Connect to the MQTT broker
            NES_DEBUG("MQTTSink: connect to host=" << host << " port=" << port);
            client.connect(connOpts)->wait();
            connected = true;
        } catch (const mqtt::exception& ex) {
            NES_ERROR("MQTTSink: " << ex.what());
        }
    }
    if (connected) {
        NES_DEBUG("MQTTSink  " << this << ": connected host=" << host << " port= " << port);
    } else {
        NES_DEBUG("MQTTSink  " << this << ": NOT connected=" << host << " port= " << port);
    }
    return connected;
}

bool MQTTSink::disconnect() {
    if (connected) {
        client.disconnect()->wait();
        connected = false;
    }
    if (!connected) {
        NES_DEBUG("MQTTSink  " << this << ": disconnected");
    } else {
        NES_DEBUG("MQTTSink  " << this << ": NOT disconnected");
    }
    return !connected;
}

int MQTTSink::getPort() { return port; }
const std::string& MQTTSink::getHost() const { return host; }
const std::string& MQTTSink::getClientId() const { return clientId; }
const std::string& MQTTSink::getTopic() const { return topic; }
const std::string& MQTTSink::getUser() const { return user; }

}// namespace NES
