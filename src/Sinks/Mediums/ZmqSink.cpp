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

#include <Sinks/Mediums/ZmqSink.hpp>

#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <zmq.hpp>

#include <NodeEngine/QueryManager.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {

SinkMediumTypes ZmqSink::getSinkMediumType() { return ZMQ_SINK; }

ZmqSink::ZmqSink(SinkFormatPtr format, const std::string& host, uint16_t port, bool internal, QuerySubPlanId parentPlanId)
    : SinkMedium(std::move(format), parentPlanId), host(host.substr(0, host.find(':'))), port(port), internal(internal),
      context(zmq::context_t(1)), socket(zmq::socket_t(context, ZMQ_PUSH)) {
    NES_DEBUG("ZmqSink  " << this << ": Init ZMQ Sink to " << host << ":" << port);
}

ZmqSink::~ZmqSink() {
    NES_DEBUG("ZmqSink::~ZmqSink: destructor called");
    bool success = disconnect();
    if (success) {
        NES_DEBUG("ZmqSink  " << this << ": Destroy ZMQ Sink");
    } else {
        /// XXX:
        NES_ERROR("ZmqSink  " << this << ": Destroy ZMQ Sink failed cause it could not be disconnected");
        throw Exception("ZMQ Sink destruction failed");
    }
    NES_DEBUG("ZmqSink  " << this << ": Destroy ZMQ Sink");
}

bool ZmqSink::writeData(NodeEngine::TupleBuffer& inputBuffer, NodeEngine::WorkerContextRef) {
    std::unique_lock lock(writeMutex);
    connect();
    if (!connected) {
        NES_DEBUG("ZmqSink  " << this << ": cannot write buffer " << inputBuffer << " because queue is not connected");
        throw Exception("Write to zmq sink failed");
    }

    if (!inputBuffer.isValid()) {
        NES_ERROR("ZmqSink::writeData input buffer invalid");
        return false;
    }

    if (!schemaWritten && !internal) {//TODO:atomic
        NES_DEBUG("ZmqSink::getData: write schema");
        auto schemaBuffer = sinkFormat->getSchema();
        if (schemaBuffer) {
            NES_DEBUG("ZmqSink writes schema buffer");
            try {
                // Send Header
                const int envelopeSize =  (sizeof(uint64_t) * 2) + sizeof(bool);
                char envelopeData;
                UtilityFunctions::fillEnvelopeBuffer(&envelopeData, true,schemaBuffer->getNumberOfTuples(), schemaBuffer->getWatermark());

                zmq::message_t envelope{&envelopeData, envelopeSize};
                if (auto const sentEnvelopeSize = socket.send(envelope, zmq::send_flags::sndmore).value_or(0);
                    sentEnvelopeSize != envelopeSize) {
                    NES_DEBUG("ZmqSink  " << this << ": schema send NOT successful");
                    return false;
                }

                // Send payload
                zmq::mutable_buffer payload{schemaBuffer->getBuffer(), schemaBuffer->getBufferSize()};
                if (auto const sentPayloadSize = socket.send(payload, zmq::send_flags::none).value_or(0);
                    sentPayloadSize != payload.size()) {
                    NES_DEBUG("ZmqSink  " << this << ": sending payload failed.");
                    return false;
                }
                NES_DEBUG("ZmqSink  " << this << ": schema send successful");

            } catch (const zmq::error_t& ex) {
                if (ex.num() != ETERM) {
                    NES_ERROR("ZmqSink: schema write " << ex.what());
                }
            }
            schemaWritten = true;
            NES_DEBUG("ZmqSink::writeData: schema written");
        }
        { NES_DEBUG("ZmqSink::writeData: no schema written"); }
    } else {
        NES_DEBUG("ZmqSink::getData: schema already written");
    }

    NES_DEBUG("ZmqSink  " << this << ": writes buffer " << inputBuffer << " with tupleCnt =" << inputBuffer.getNumberOfTuples()
                          << " watermark=" << inputBuffer.getWatermark());
    auto dataBuffers = sinkFormat->getData(inputBuffer);
    for (auto buffer : dataBuffers) {// XXX: Is it actually our intention to iterate over buffers until no exception is thrown?
        try {
            ++sentBuffer;

            // Create envelope
            const int envelopeSize =  (sizeof(uint64_t) * 2) + sizeof(bool);
            char envelopeData[envelopeSize];
            UtilityFunctions::fillEnvelopeBuffer(envelopeData, false, buffer.getNumberOfTuples(), buffer.getWatermark());
            zmq::message_t envelope{&(envelopeData), sizeof(envelopeData)};
            if (auto const sentEnvelope = socket.send(envelope, zmq::send_flags::sndmore).value_or(0);
                sentEnvelope != sizeof(envelopeData)) {
                NES_WARNING("ZmqSink  " << this << ": data payload send NOT successful");
                return false;
            }

            // Create message.
            // Copying the entire payload here to avoid UB.
            zmq::message_t payload{buffer.getBuffer(), buffer.getBufferSize()};
            if (auto const sentPayload = socket.send(payload, zmq::send_flags::none).value_or(0);
                sentPayload != buffer.getBufferSize()) {
                NES_WARNING("ZmqSink  " << this << ": data send NOT successful");
                return false;
            }
            NES_DEBUG("ZmqSink  " << this << ": data send successful");
            return true;

        } catch (const zmq::error_t& ex) {
            // recv() throws ETERM when the zmq context is destroyed,
            //  as when AsyncZmqListener::Stop() is called
            if (ex.num() != ETERM) {
                NES_ERROR("ZmqSink: " << ex.what());
            }
        }
    }
    return true;
}

std::string ZmqSink::toString() const {
    std::stringstream ss;
    ss << "ZMQ_SINK(";
    ss << "SCHEMA(" << sinkFormat->getSchemaPtr()->toString() << "), ";
    ss << "HOST=" << host << ", ";
    ss << "PORT=" << port;
    ss << ")";
    return ss.str();
}

bool ZmqSink::connect() {
    if (!connected) {
        try {
            NES_DEBUG("ZmqSink: connect to address=" << host << " port=" << port);
            auto address = std::string("tcp://") + host + std::string(":") + std::to_string(port);
            socket.connect(address.c_str());
            connected = true;
        } catch (const zmq::error_t& ex) {
            // recv() throws ETERM when the zmq context is destroyed,
            //  as when AsyncZmqListener::Stop() is called
            if (ex.num() != ETERM) {
                NES_ERROR("ZmqSink: " << ex.what());
            }
        }
    }
    if (connected) {
        NES_DEBUG("ZmqSink  " << this << ": connected address=" << host << " port= " << port);
    } else {
        NES_DEBUG("ZmqSink  " << this << ": NOT connected=" << host << " port= " << port);
    }
    return connected;
}

bool ZmqSink::disconnect() {
    if (connected) {
        socket.close();
        connected = false;
    }
    if (!connected) {
        NES_DEBUG("ZmqSink  " << this << ": disconnected");
    } else {
        NES_DEBUG("ZmqSink  " << this << ": NOT disconnected");
    }
    return !connected;
}

int ZmqSink::getPort() const { return this->port; }

std::string ZmqSink::getHost() const { return host; }

}// namespace NES
