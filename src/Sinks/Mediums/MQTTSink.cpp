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

#include "mqtt/client.h"
#include "mqtt/async_client.h"

namespace NES {
std::string MQTTSink::toString() { return "MQTT_SINK"; }

SinkMediumTypes MQTTSink::getSinkMediumType() { return MQTT_SINK; }

//TODO IMPLEMENT CONNECT! Rethink parameter choice!

//TODO find out which parameters are needed by MQTT
// - SinkFormatPtr? [y] -> which kind of data does arrive at sink
// - QuerySubPlanId [y/n] -> ? not sure, for debugging? ASK
// - Schema         [n]   -> probably not required
// - NodeEngginePtr [n]   -> probably not required
// might want to change host(host) -> host(host.substr(0, host.find(":")))
MQTTSink::MQTTSink(SinkFormatPtr sink_format, QuerySubPlanId parentPlanId, std::string& host,
                   uint16_t port, std::string& user, std::string& password)
    : SinkMedium(sink_format, parentPlanId), host(host), port(port), user(user), password(password), connected(false) {
    NES_DEBUG("MQTT Sink  " << this << ": Init MQTT Sink to " << host << ":" << port);
}
//MQTTSink::MQTTSink(SinkFormatPtr format, const std::string& host, const uint16_t port, bool internal, QuerySubPlanId parentPlanId)
//    : SinkMedium(format, parentPlanId), host(host.substr(0, host.find(":"))), port(port), connected(false), internal(internal),
//      context(MQTT::context_t(1)), socket(MQTT::socket_t(context, ZMQ_PUSH)) {
//    NES_DEBUG("MQTT Sink  " << this << ": Init MQTT Sink to " << host << ":" << port);
//}

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
    std::unique_lock lock(writeMutex);
    connect();
    if (!connected) {
        NES_DEBUG("MQTTSink  " << this << ": cannot write buffer " << inputBuffer << " because queue is not connected");
        throw Exception("Write to MQTT sink failed");
    }

    if (!inputBuffer.isValid()) {
        NES_ERROR("MQTTSink::writeData input buffer invalid");
        return false;
    }

    if (!schemaWritten) {//TODO:atomic
        NES_DEBUG("FileSink::getData: write schema");
        auto schemaBuffer = sinkFormat->getSchema();
        if (schemaBuffer) {
            NES_DEBUG("MQTTSink writes schema buffer");
            try {
                // TODO insert MQTT write code
                //  what is the idea of write code?
                //  -

                //	uint64_t usedBufferSize = inputBuffer->num_tuples * inputBuffer->tuple_size_bytes;
                MQTT::message_t msg(schemaBuffer->getBufferSize());

                std::memcpy(msg.data(), schemaBuffer->getBuffer(), schemaBuffer->getBufferSize());
                MQTT::message_t envelope(sizeof(uint64_t));
                uint64_t schemaSize = schemaBuffer->getNumberOfTuples();
                memcpy(envelope.data(), &schemaSize, sizeof(schemaSize));

                bool rc_env = socket.send(envelope, ZMQ_SNDMORE);
                bool rc_msg = socket.send(msg);
                if (!rc_env || !rc_msg) {
                    NES_DEBUG("MQTTSink  " << this << ": schema send NOT successful");
                    return false;
                } else {
                    NES_DEBUG("MQTTSink  " << this << ": schema send successful");
                }
            } catch (const MQTT::error_t& ex) {
                if (ex.num() != ETERM) {
                    NES_ERROR("MQTTSink: schema write " << ex.what());
                }
            }
            schemaWritten = true;
            NES_DEBUG("MQTTSink::writeData: schema written");
        }
        { NES_DEBUG("MQTTSink::writeData: no schema written"); }
    } else {
        NES_DEBUG("MQTTSink::getData: schema already written");
    }

    NES_DEBUG("MQTTSink  " << this << ": writes buffer " << inputBuffer << " with tupleCnt =" << inputBuffer.getNumberOfTuples()
                          << " watermark=" << inputBuffer.getWatermark());
    auto dataBuffers = sinkFormat->getData(inputBuffer);
    for (auto buffer : dataBuffers) {
        try {
            uint64_t tupleCnt = buffer.getNumberOfTuples();
            uint64_t currentTs = buffer.getWatermark();

            MQTT::message_t envelope(sizeof(tupleCnt) + sizeof(currentTs));
            memcpy(envelope.data(), &tupleCnt, sizeof(tupleCnt));
            memcpy((void*) ((char*) envelope.data() + sizeof(currentTs)), &currentTs, sizeof(currentTs));

            MQTT::message_t msg(buffer.getBufferSize());
            std::memcpy(msg.data(), buffer.getBuffer(), buffer.getBufferSize());

            bool rc_env = socket.send(envelope, ZMQ_SNDMORE);
            bool rc_msg = socket.send(msg);
            sentBuffer++;
            if (!rc_env || !rc_msg) {
                NES_DEBUG("MQTTSink  " << this << ": data send NOT successful");
                return false;
            } else {
                NES_DEBUG("MQTTSink  " << this << ": data send successful");
                return true;
            }
        } catch (const MQTT::error_t& ex) {
            // recv() throws ETERM when the MQTT context is destroyed,
            //  as when AsyncMQTTListener::Stop() is called
            if (ex.num() != ETERM) {
                NES_ERROR("MQTTSink: " << ex.what());
            }
        }
    }
    return true;
}

//TODO add more to print?
const std::string MQTTSink::toString() const {
    std::stringstream ss;
    ss << "MQTT_SINK(";
    ss << "SCHEMA(" << sinkFormat->getSchemaPtr()->toString() << "), ";
    ss << "HOST=" << host << ", ";
    ss << "PORT=" << port << ", ";
    ss << "\")";
    return ss.str();
}

bool MQTTSink::connect() {
    if (!connected) {
        try {
            NES_DEBUG("MQTTSink: connect to host=" << host << " port=" << port);
            auto address = std::string("tcp://") + host + std::string(":") + std::to_string(port);
            socket.connect(address.c_str());
            connected = true;
        } catch (const MQTT::error_t& ex) {
            // recv() throws ETERM when the MQTT context is destroyed,
            //  as when AsyncMQTTListener::Stop() is called
            if (ex.num() != ETERM) {
                NES_ERROR("MQTTSink: " << ex.what());
            }
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
        socket.close();
        connected = false;
    }
    if (!connected) {
        NES_DEBUG("MQTTSink  " << this << ": disconnected");
    } else {
        NES_DEBUG("MQTTSink  " << this << ": NOT disconnected");
    }
    return !connected;
}

int MQTTSink::getPort() { return this->port; }

const std::string MQTTSink::getHost() const { return host; }

}// namespace NES
