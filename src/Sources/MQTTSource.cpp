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

#include "mqtt/client.h"
#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <Sources/MQTTSource.hpp>
#include <Util/Logger.hpp>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <sstream>
#include <string>

using namespace std;
using namespace std::chrono;

namespace NES {

MQTTSource::MQTTSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager, NodeEngine::QueryManagerPtr queryManager,
                       const std::string serverAddress, const std::string clientId, OperatorId operatorId)
    : DataSource(schema, bufferManager, queryManager, operatorId), connected(false), serverAddress(serverAddress),
      clientId(clientId) {
    NES_DEBUG("MQTTSOURCE  " << this << ": Init MQTTSOURCE to " << serverAddress << " with client id: " << clientId << "/");
}

MQTTSource::~MQTTSource() {
    NES_DEBUG("MQTTSource::~MQTTSource()");
    bool success = disconnect();
    if (success) {
        NES_DEBUG("MQTTSOURCE  " << this << ": Destroy MQTT Source");
    } else {
        NES_ERROR("MQTTSOURCE  " << this << ": Destroy MQTT Source failed cause it could not be disconnected");
        assert(0);
    }
    NES_DEBUG("MQTTSOURCE  " << this << ": Destroy MQTT Source");
}

std::optional<NodeEngine::TupleBuffer> MQTTSource::receiveData() {
    NES_DEBUG("MQTTSOURCE  " << this << ": receiveData ");

    if (connect()) {// was if (connect()) {
        try {

            return buffer;
        } catch (const zmq::error_t& ex) {
            NES_ERROR("MQTTSOURCE error: " << ex.what());
            return std::nullopt;
        } catch (...) {
            NES_ERROR("MQTTSOURCE general error");
            return std::nullopt;
        }
    } else {
        NES_ERROR("MQTTSOURCE: Not connected!");
        return std::nullopt;
    }
}

const std::string MQTTSource::toString() const {
    std::stringstream ss;
    ss << "MQTTSOURCE(";
    ss << "SCHEMA(" << schema->toString() << "), ";
    ss << "HOST=" << host << ", ";
    ss << "PORT=" << port << ", ";
    return ss.str();
}

bool MQTTSource::connect() {
    if (!connected) {
        NES_DEBUG("MQTTSOURCE was !conncect now connect " << this << ": connected");

        mqtt::client cli(serverAddress, clientId);

        auto connOpts = mqtt::connect_options_builder()
                            .user_name("user")
                            .password("passwd")
                            .keep_alive_interval(seconds(30))
                            .automatic_reconnect(seconds(2), seconds(30))
                            .clean_session(false)
                            .finalize();

        if (connected) {
            NES_DEBUG("MQTTSOURCE  " << this << ": connected");
        } else {
            NES_DEBUG("Exception: MQTTSOURCE  " << this << ": NOT connected");
        }
        return connected;
    }
}

bool MQTTSource::disconnect() {
    NES_DEBUG("MQTTSOURCE::disconnect() connected=" << connected);
    if (connected) {
        // we put assert here because it d be called anyway from the shutdown method
        // that we commented out

        if (!success) {
            throw Exception("MQTTSOURCE::disconnect() error");
        }
        //        context.shutdown();
        connected = false;
    }
    if (!connected) {
        NES_DEBUG("MQTTSOURCE  " << this << ": disconnected");
    } else {
        NES_DEBUG("MQTTSOURCE  " << this << ": NOT disconnected");
    }
    return !connected;
}

}// namespace NES
