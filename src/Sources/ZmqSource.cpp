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

#include <Sources/ZmqSource.hpp>

#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <Util/Logger.hpp>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <sstream>
#include <string>
#include <zmq.hpp>

namespace NES {

ZmqSource::ZmqSource(SchemaPtr schema,
                     BufferManagerPtr bufferManager,
                     QueryManagerPtr queryManager,
                     const std::string& host,
                     const uint16_t port, OperatorId operatorId)
    : DataSource(schema, bufferManager, queryManager, operatorId),
      host(host.substr(0, host.find(":"))),
      port(port),
      connected(false),
      context(zmq::context_t(1)),
      socket(zmq::socket_t(context, ZMQ_PULL)) {
    NES_DEBUG("ZMQSOURCE  " << this << ": Init ZMQ ZMQSOURCE to " << host << ":" << port << "/");
}

ZmqSource::~ZmqSource() {
    NES_DEBUG("ZmqSource::~ZmqSource()");
    bool success = disconnect();
    if (success) {
        NES_DEBUG("ZMQSOURCE  " << this << ": Destroy ZMQ Source");
    } else {
        NES_ERROR("ZMQSOURCE  " << this << ": Destroy ZMQ Source failed cause it could not be disconnected");
        assert(0);
    }
    NES_DEBUG("ZMQSOURCE  " << this << ": Destroy ZMQ Source");
}

std::optional<TupleBuffer> ZmqSource::receiveData() {
    NES_DEBUG("ZMQSource  " << this << ": receiveData ");
    if (connect()) {// was if (connect()) {
        try {
            // Receive new chunk of data
            zmq::message_t new_data;
            socket.recv(&new_data);// envelope - not needed at the moment
            size_t tupleCnt = *((size_t*) new_data.data());
            size_t currentTs = *(((size_t*) new_data.data()) + 1);
            NES_DEBUG("ZMQSource received #tups " << tupleCnt << " watermark=" << currentTs);

            zmq::message_t new_data2;
            socket.recv(&new_data2);// actual data

            auto buffer = bufferManager->getBufferBlocking();
            buffer.setWatermark(currentTs);
            NES_DEBUG("ZMQSource  " << this << ": got buffer ");

            // TODO: If possible only copy the content not the empty part
            std::memcpy(buffer.getBuffer(), new_data2.data(), buffer.getBufferSize());
            buffer.setNumberOfTuples(tupleCnt);

            if (buffer.getBufferSize() == new_data2.size()) {
                NES_WARNING("ZMQSource  " << this << ": return buffer");
            }
            return buffer;
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

const std::string ZmqSource::toString() const {
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
            socket.setsockopt(ZMQ_LINGER, 0);
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

const std::string& ZmqSource::getHost() const {
    return host;
}

uint16_t ZmqSource::getPort() const {
    return port;
}

}// namespace NES
