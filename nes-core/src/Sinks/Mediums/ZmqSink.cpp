/*
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

#include <Runtime/QueryManager.hpp>
#include <Sinks/Mediums/ZmqSink.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstdint>

#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <zmq.hpp>

namespace NES {

SinkMediumTypes ZmqSink::getSinkMediumType() { return ZMQ_SINK; }

ZmqSink::ZmqSink(SinkFormatPtr format,
                 Runtime::NodeEnginePtr nodeEngine,
                 uint32_t numOfProducers,
                 const std::string& host,
                 uint16_t port,
                 bool internal,
                 QueryId queryId,
                 QuerySubPlanId querySubPlanId,
                 FaultToleranceType::Value faultToleranceType,
                 uint64_t numberOfOrigins)
    : SinkMedium(std::move(format),
                 std::move(nodeEngine),
                 numOfProducers,
                 queryId,
                 querySubPlanId,
                 faultToleranceType,
                 numberOfOrigins,
                 std::make_unique<Windowing::MultiOriginWatermarkProcessor>(numberOfOrigins)),
      host(host.substr(0, host.find(':'))), port(port), internal(internal), context(zmq::context_t(1)),
      socket(zmq::socket_t(context, ZMQ_PUSH)) {
    NES_DEBUG2("ZmqSink: Init ZMQ Sink to {}:{}", host,port);
}

void ZmqSink::setup() { connect(); };
void ZmqSink::shutdown(){};

ZmqSink::~ZmqSink() {
    NES_DEBUG2("ZmqSink::~ZmqSink: destructor called");
    bool success = disconnect();
    if (success) {
        NES_DEBUG2("ZmqSink: Destroy ZMQ Sink");
    } else {
        /// XXX:
        NES_ERROR2("ZmqSink: Destroy ZMQ Sink failed cause it could not be disconnected");
        NES_ASSERT2_FMT(false, "ZMQ Sink destruction failed");
    }
    NES_DEBUG2("ZmqSink: Destroy ZMQ Sink");
}

bool ZmqSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) {
    std::unique_lock lock(writeMutex);// TODO this is an anti-pattern in ZMQ
    connect();
    if (!connected) {
        NES_DEBUG2("ZmqSink: cannot write buffer because queue is not connected");
        throw Exceptions::RuntimeException("Write to zmq sink failed");
    }

    if (!inputBuffer) {
        NES_ERROR2("ZmqSink::writeData input buffer invalid");
        return false;
    }

    if (!schemaWritten && !internal) {//TODO:atomic
        NES_DEBUG2("FileSink::getData: write schema");
        auto schemaBuffer = sinkFormat->getSchema();
        if (schemaBuffer) {
            NES_DEBUG2("ZmqSink writes schema buffer");
            try {

                // Send Header
                std::array<uint64_t, 2> const envelopeData{schemaBuffer->getNumberOfTuples(), schemaBuffer->getWatermark()};
                constexpr auto envelopeSize = sizeof(uint64_t) * 2;
                static_assert(envelopeSize == sizeof(envelopeData));
                zmq::message_t envelope{&(envelopeData[0]), envelopeSize};
                if (auto const sentEnvelopeSize = socket.send(envelope, zmq::send_flags::sndmore).value_or(0);
                    sentEnvelopeSize != envelopeSize) {
                    NES_DEBUG2("ZmqSink: schema send NOT successful");
                    return false;
                }

                // Send payload
                zmq::mutable_buffer payload{schemaBuffer->getBuffer(), schemaBuffer->getBufferSize()};
                if (auto const sentPayloadSize = socket.send(payload, zmq::send_flags::none).value_or(0);
                    sentPayloadSize != payload.size()) {
                    NES_DEBUG2("ZmqSink: sending payload failed.");
                    return false;
                }
                NES_DEBUG2("ZmqSink: schema send successful");

            } catch (const zmq::error_t& ex) {
                if (ex.num() != ETERM) {
                    NES_ERROR2("ZmqSink: schema write  {}",  ex.what());
                }
            }
            schemaWritten = true;
            NES_DEBUG2("ZmqSink::writeData: schema written");
        }
        { NES_DEBUG2("ZmqSink::writeData: no schema written"); }
    } else {
        NES_DEBUG2("ZmqSink::getData: schema already written");
    }

    NES_DEBUG2("ZmqSink: writes buffer with tupleCnt ={} watermark={}", inputBuffer.getNumberOfTuples(), inputBuffer.getWatermark());
    auto dataBuffers = sinkFormat->getData(inputBuffer);
    for (auto buffer : dataBuffers) {// XXX: Is it actually our intention to iterate over buffers until no exception is thrown?
        try {
            ++sentBuffer;

            // Create envelope
            std::array<uint64_t, 2> const envelopeData{buffer.getNumberOfTuples(), buffer.getWatermark()};
            static_assert(sizeof(envelopeData) == sizeof(uint64_t) * 2);
            zmq::message_t envelope{&(envelopeData[0]), sizeof(envelopeData)};
            if (auto const sentEnvelope = socket.send(envelope, zmq::send_flags::sndmore).value_or(0);
                sentEnvelope != sizeof(envelopeData)) {
                NES_WARNING2("ZmqSink: data payload send NOT successful");
                return false;
            }

            // Create message.
            // Copying the entire payload here to avoid UB.
            zmq::message_t payload{buffer.getBuffer(), buffer.getBufferSize()};
            if (auto const sentPayload = socket.send(payload, zmq::send_flags::none).value_or(0);
                sentPayload != buffer.getBufferSize()) {
                NES_WARNING2("ZmqSink: data send NOT successful");
                return false;
            }
            NES_DEBUG2("ZmqSink: data send successful");
            return true;

        } catch (const zmq::error_t& ex) {
            // recv() throws ETERM when the zmq context is destroyed,
            //  as when AsyncZmqListener::Stop() is called
            if (ex.num() != ETERM) {
                NES_ERROR2("ZmqSink:  {}",  ex.what());
            }
        }
    }
    updateWatermarkCallback(inputBuffer);
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
            NES_DEBUG2("ZmqSink: connect to address= {}  port= {}",  host,  port);
            auto address = std::string("tcp://") + host + std::string(":") + std::to_string(port);
            socket.connect(address.c_str());
            connected = true;
        } catch (const zmq::error_t& ex) {
            // recv() throws ETERM when the zmq context is destroyed,
            //  as when AsyncZmqListener::Stop() is called
            if (ex.num() != ETERM) {
                NES_ERROR2("ZmqSink:  {}",  ex.what());
            }
        }
    }
    if (connected) {
        NES_DEBUG2("ZmqSink : connected address= {}  port=  {}", host, port);
    } else {
        NES_DEBUG2("ZmqSink : NOT connected= {}  port=  {}", host, port);
    }
    return connected;
}

bool ZmqSink::disconnect() {
    if (connected) {
        socket.close();
        connected = false;
    }
    if (!connected) {
        NES_DEBUG2("ZmqSink : disconnected");
    } else {
        NES_DEBUG2("ZmqSink : NOT disconnected");
    }
    return !connected;
}

int ZmqSink::getPort() const { return this->port; }

std::string ZmqSink::getHost() const { return host; }

}// namespace NES
