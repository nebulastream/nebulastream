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

#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sources/ZmqSource.hpp>
#include <Util/Logger.hpp>
#include <cstdint>
#include <cstring>
#include <sstream>
#include <string>
#include <utility>
#include <zmq.hpp>

namespace NES {

ZmqSource::ZmqSource(SchemaPtr schema,
                     Runtime::BufferManagerPtr bufferManager,
                     Runtime::QueryManagerPtr queryManager,
                     const std::string& host,
                     uint16_t port,
                     OperatorId operatorId,
                     uint64_t numSourceLocalBuffers,
                     GatheringMode gatheringMode,
                     std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
    : DataSource(std::move(schema),
                 std::move(bufferManager),
                 std::move(queryManager),
                 operatorId,
                 numSourceLocalBuffers,
                 gatheringMode,
                 std::move(successors)),
      host(host), port(port), connected(false), context(zmq::context_t(1)), socket(zmq::socket_t(context, ZMQ_PULL)) {
    NES_DEBUG("ZMQSOURCE  " << this << ": Init ZMQ ZMQSOURCE to " << host << ":" << port << "/");
}

ZmqSource::~ZmqSource() NES_NOEXCEPT(false) {
    NES_DEBUG("ZmqSource::~ZmqSource()");
    bool success = disconnect();
    if (success) {
        NES_DEBUG("ZMQSOURCE  " << this << ": Destroy ZMQ Source");
    } else {
        NES_ASSERT2_FMT(false, "ZMQSOURCE  " << this << ": Destroy ZMQ Source failed cause it could not be disconnected");
    }
    NES_DEBUG("ZMQSOURCE  " << this << ": Destroy ZMQ Source");
}

std::optional<Runtime::TupleBuffer> ZmqSource::receiveData() {
    NES_DEBUG("ZMQSource  " << this << ": receiveData ");
    if (connect()) {
        try {

            // Receive metadata
            auto const metadataSize = sizeof(uint64_t) * 2;
            zmq::message_t metadata{metadataSize};

            // TODO: Clarify following comment: envelope - not needed at the moment
            if (auto const receivedSize = socket.recv(metadata).value_or(0); receivedSize != metadataSize) {
                NES_ERROR("ZMQSource: Error: Unexpected payload size. Expected: " << metadataSize
                                                                                  << " Received: " << receivedSize);
                return std::nullopt;
            }

            auto buffer = bufferManager->getBufferBlocking();
            buffer.setNumberOfTuples(static_cast<uint64_t*>(metadata.data())[0]);
            buffer.setWatermark(static_cast<uint64_t*>(metadata.data())[1]);
            NES_DEBUG("ZMQSource received #tups " << buffer.getNumberOfTuples() << " watermark=" << buffer.getWatermark());

            // Receive payload
            // XXX: I guess we don't actually know the size here, it would be nice to be able to check that here
            zmq::mutable_buffer payload{buffer.getBuffer(), buffer.getBufferSize()};
            if (auto const receivedSize = socket.recv(payload); !receivedSize.has_value()) {
                NES_ERROR("ZMQSource: Error: Unexpected payload size. Expected: " << buffer.getBufferSize()
                                                                                  << " Received: " << receivedSize.has_value());
                return std::nullopt;
            } else {
                NES_DEBUG("ZMQSource  " << this << ": received buffer of size " << receivedSize.has_value());
                return buffer;
            }

        } catch (const zmq::error_t& ex) {
            NES_ERROR("ZMQSOURCE error: " << ex.what());
            return std::nullopt;
        } catch (...) {
            NES_ERROR("ZMQSOURCE general error");
            return std::nullopt;
        }
    } else {
        NES_ERROR("ZMQSOURCE: Not connected!");
        return std::nullopt;
    }
}

std::string ZmqSource::toString() const {
    std::stringstream ss;
    ss << "ZMQ_SOURCE(";
    ss << "SCHEMA(" << schema->toString() << "), ";
    ss << "HOST=" << host << ", ";
    ss << "PORT=" << port << ", ";
    return ss.str();
}

bool ZmqSource::connect() {
    if (!connected) {
        NES_DEBUG("ZMQSOURCE was !conncect now connect " << this << ": connected");
        if (host == "localhost") {
            host = "*";
        }
        auto address = std::string("tcp://") + host + std::string(":") + std::to_string(port);
        NES_DEBUG("ZMQSOURCE use address " << address);
        try {
            socket.set(zmq::sockopt::linger, 0);
            socket.bind(address.c_str());
            NES_DEBUG("ZMQSOURCE  " << this << ": set connected true");
            connected = true;
        } catch (const zmq::error_t& ex) {
            // recv() throws ETERM when the zmq context is destroyed,
            //  as when AsyncZmqListener::Stop() is called
            if (ex.num() != ETERM) {
                NES_ERROR("ZMQSOURCE ERROR: " << ex.what());
                NES_DEBUG("ZMQSOURCE  " << this << ": set connected false");
            }
            connected = false;
        }
    }

    if (connected) {
        NES_DEBUG("ZMQSOURCE  " << this << ": connected");
    } else {
        NES_DEBUG("Exception: ZMQSOURCE  " << this << ": NOT connected");
    }
    return connected;
}

bool ZmqSource::disconnect() {
    NES_DEBUG("ZmqSource::disconnect() connected=" << connected);
    if (connected) {
        // we put assert here because it d be called anyway from the shutdown method
        // that we commented out
        bool success = zmq_ctx_shutdown(static_cast<void*>(context)) == 0;
        if (!success) {
            throw Exception("ZmqSource::disconnect() error");
        }
        //        context.shutdown();
        connected = false;
    }
    if (!connected) {
        NES_DEBUG("ZMQSOURCE  " << this << ": disconnected");
    } else {
        NES_DEBUG("ZMQSOURCE  " << this << ": NOT disconnected");
    }
    return !connected;
}

SourceType ZmqSource::getType() const { return ZMQ_SOURCE; }

const std::string& ZmqSource::getHost() const { return host; }

uint16_t ZmqSource::getPort() const { return port; }

}// namespace NES
