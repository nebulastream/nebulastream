#include <Sinks/Mediums/ZmqSink.hpp>

#include <cassert>
#include <cstdint>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <zmq.hpp>

#include <NodeEngine/QueryManager.hpp>
#include <Util/Logger.hpp>

namespace NES {
std::string ZmqSink::toString() {
    return "ZMQ_SINK";
}

SinkMediumTypes ZmqSink::getSinkMediumType() {
    return ZMQ_SINK;
}

ZmqSink::ZmqSink(SinkFormatPtr format,
                 const std::string& host,
                 const uint16_t port,
                 bool internal,
                 QuerySubPlanId parentPlanId)
    : SinkMedium(format, parentPlanId),
      host(host.substr(0, host.find(":"))),
      port(port),
      connected(false),
      internal(internal),
      context(zmq::context_t(1)),
      socket(zmq::socket_t(context, ZMQ_PUSH)) {
    NES_DEBUG(
        "ZmqSink  " << this << ": Init ZMQ Sink to " << host << ":" << port);
}

ZmqSink::~ZmqSink() {
    NES_DEBUG("ZmqSink::~ZmqSink: destructor called");
    bool success = disconnect();
    if (success) {
        NES_DEBUG("ZmqSink  " << this << ": Destroy ZMQ Sink");
    } else {
        NES_ERROR(
            "ZmqSink  " << this << ": Destroy ZMQ Sink failed cause it could not be disconnected");
        throw Exception("ZMQ Sink destruction failed");
    }
    NES_DEBUG("ZmqSink  " << this << ": Destroy ZMQ Sink");
}

bool ZmqSink::writeData(TupleBuffer& inputBuffer, WorkerContextRef) {
    std::unique_lock lock(writeMutex);
    connect();
    if (!connected) {
        NES_DEBUG(
            "ZmqSink  " << this << ": cannot write buffer " << inputBuffer << " because queue is not connected");
        throw Exception("Write to zmq sink failed");
    }

    if (!inputBuffer.isValid()) {
        NES_ERROR("ZmqSink::writeData input buffer invalid");
        return false;
    }

    if (!schemaWritten && !internal) {//TODO:atomic
        NES_DEBUG("FileSink::getData: write schema");
        auto schemaBuffer = sinkFormat->getSchema();
        if (schemaBuffer) {
            NES_DEBUG("ZmqSink writes schema buffer");
            try {
                //	size_t usedBufferSize = inputBuffer->num_tuples * inputBuffer->tuple_size_bytes;
                zmq::message_t msg(schemaBuffer->getBufferSize());
                // TODO: If possible only copy the content not the empty part
                std::memcpy(msg.data(), schemaBuffer->getBuffer(), schemaBuffer->getBufferSize());
                zmq::message_t envelope(sizeof(size_t));
                size_t schemaSize = schemaBuffer->getNumberOfTuples();
                memcpy(envelope.data(), &schemaSize, sizeof(schemaSize));

                bool rc_env = socket.send(envelope, ZMQ_SNDMORE);
                bool rc_msg = socket.send(msg);
                if (!rc_env || !rc_msg) {
                    NES_DEBUG("ZmqSink  " << this << ": schema send NOT successful");
                    return false;
                } else {
                    NES_DEBUG("ZmqSink  " << this << ": schema send successful");
                }
            } catch (const zmq::error_t& ex) {
                if (ex.num() != ETERM) {
                    NES_ERROR("ZmqSink: schema write " << ex.what());
                }
            }
            schemaWritten = true;
            NES_DEBUG("ZmqSink::writeData: schema written");
        }
        {
            NES_DEBUG("ZmqSink::writeData: no schema written");
        }
    } else {
        NES_DEBUG("ZmqSink::getData: schema already written");
    }

    NES_DEBUG("ZmqSink  " << this << ": writes buffer " << inputBuffer
                          << " with tupleCnt =" << inputBuffer.getNumberOfTuples() << " watermark=" << inputBuffer.getWatermark());
    auto dataBuffers = sinkFormat->getData(inputBuffer);
    for (auto buffer : dataBuffers) {
        try {
            size_t tupleCnt = buffer.getNumberOfTuples();
            size_t currentTs = buffer.getWatermark();

            zmq::message_t envelope(sizeof(tupleCnt) + sizeof(currentTs));
            memcpy(envelope.data(), &tupleCnt, sizeof(tupleCnt));
            memcpy((void*) ((char*) envelope.data() + sizeof(currentTs)), &currentTs, sizeof(currentTs));

            zmq::message_t msg(buffer.getBufferSize());
            std::memcpy(msg.data(), buffer.getBuffer(), buffer.getBufferSize());

            bool rc_env = socket.send(envelope, ZMQ_SNDMORE);
            bool rc_msg = socket.send(msg);
            sentBuffer++;
            if (!rc_env || !rc_msg) {
                NES_DEBUG("ZmqSink  " << this << ": data send NOT successful");
                return false;
            } else {
                NES_DEBUG("ZmqSink  " << this << ": data send successful");
                return true;
            }
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

const std::string ZmqSink::toString() const {
    std::stringstream ss;
    ss << "ZMQ_SINK(";
    ss << "SCHEMA(" << sinkFormat->getSchemaPtr()->toString() << "), ";
    ss << "HOST=" << host << ", ";
    ss << "PORT=" << port << ", ";
    ss << "\")";
    return ss.str();
}

bool ZmqSink::connect() {
    if (!connected) {
        try {
            NES_DEBUG("ZmqSink: connect to host=" << host << " port=" << port);
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

        NES_DEBUG("ZmqSink  " << this << ": connected host=" << host << " port= " << port);
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

int ZmqSink::getPort() {
    return this->port;
}

const std::string ZmqSink::getHost() const {
    return host;
}

}// namespace NES
