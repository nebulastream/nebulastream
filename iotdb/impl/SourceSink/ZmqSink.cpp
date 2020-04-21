#include <SourceSink/ZmqSink.hpp>

#include <cassert>
#include <cstdint>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <zmq.hpp>

#include <NodeEngine/Dispatcher.hpp>
#include <Util/Logger.hpp>

#include <Util/Logger.hpp>

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>

namespace NES {

ZmqSink::ZmqSink()
        : host(""),
          port(0),
          tupleCnt(0),
          connected(false),
          context(zmq::context_t(1)),
          socket(zmq::socket_t(context, ZMQ_PUSH)) {
    NES_DEBUG(
            "DEFAULT ZMQSINK  " << this << ": Init ZMQ Sink to " << host << ":" << port)
}

ZmqSink::ZmqSink(SchemaPtr schema, const std::string &host,
                 const uint16_t port)
    : DataSink(schema),
      host(host),
      port(port),
      tupleCnt(0),
      connected(false),
      context(zmq::context_t(1)),
      socket(zmq::socket_t(context, ZMQ_PUSH)) {
    NES_DEBUG(
        "ZMQSINK  " << this << ": Init ZMQ Sink to " << host << ":" << port)
}

ZmqSink::~ZmqSink() {
    NES_DEBUG("ZmqSink::~ZmqSink: destructor called")
    bool success = disconnect();
    if (success) {
        NES_DEBUG("ZMQSINK  " << this << ": Destroy ZMQ Sink")
    } else {
        NES_ERROR(
            "ZMQSINK  " << this << ": Destroy ZMQ Sink failed cause it could not be disconnected")
        assert(0);
    }
    NES_DEBUG("ZMQSINK  " << this << ": Destroy ZMQ Sink")

}

bool ZmqSink::writeData(TupleBuffer& input_buffer) {
//    bool connected =
        connect();
    if (!connected) {
        NES_DEBUG(
            "ZMQSINK  " << this << ": cannot write buffer " << input_buffer << " because queue is not connected")
        assert(0);
    }

    NES_DEBUG("ZMQSINK  " << this << ": writes buffer " << input_buffer)
    try {
        //	size_t usedBufferSize = input_buffer->num_tuples * input_buffer->tuple_size_bytes;
        zmq::message_t msg(input_buffer.getBufferSize());
        // TODO: If possible only copy the content not the empty part
        std::memcpy(msg.data(), input_buffer.getBuffer(), input_buffer.getBufferSize());
        tupleCnt = input_buffer.getNumberOfTuples();
        zmq::message_t envelope(sizeof(tupleCnt));
        memcpy(envelope.data(), &tupleCnt, sizeof(tupleCnt));

        bool rc_env = socket.send(envelope, ZMQ_SNDMORE);
        bool rc_msg = socket.send(msg);
        sentBuffer++;
        if (!rc_env || !rc_msg) {
            NES_DEBUG("ZMQSINK  " << this << ": send NOT successful")
            return false;
        } else {
            NES_DEBUG("ZMQSINK  " << this << ": send successful")
            return true;
        }
    }
    catch (const zmq::error_t& ex) {
        // recv() throws ETERM when the zmq context is destroyed,
        //  as when AsyncZmqListener::Stop() is called
        if (ex.num() != ETERM) {
            NES_ERROR("ZmqSink: " << ex.what())
        }
    }
    return false;
}

const std::string ZmqSink::toString() const {
  std::stringstream ss;
  ss << "ZMQ_SINK(";
  ss << "SCHEMA(" << schema->toString() << "), ";
  ss << "HOST=" << host << ", ";
  ss << "PORT=" << port << ", ";
  ss << "TupleCnt=\"" << tupleCnt << "\")";
  return ss.str();
}

bool ZmqSink::connect() {
    if (!connected) {
        try {
            NES_DEBUG("ZmqSink: connect to host=" << host << " port=" << port)
            auto address = std::string("tcp://") + host + std::string(":") + std::to_string(port);
            socket.connect(address.c_str());
            connected = true;
        }
        catch (const zmq::error_t& ex) {
            // recv() throws ETERM when the zmq context is destroyed,
            //  as when AsyncZmqListener::Stop() is called
            if (ex.num() != ETERM) {
                NES_ERROR("ZmqSink: " << ex.what())
            }
        }
    }
    if (connected) {

        NES_DEBUG("ZMQSINK  " << this << ": connected host=" << host << " port= " << port)
    } else {
        NES_DEBUG("ZMQSINK  " << this << ": NOT connected=" << host << " port= " << port)
    }
    return connected;
}

bool ZmqSink::disconnect() {
    if (connected) {
        socket.close();
        connected = false;
    }
    if (!connected) {
        NES_DEBUG("ZMQSINK  " << this << ": disconnected")
    } else {
        NES_DEBUG("ZMQSINK  " << this << ": NOT disconnected")
    }
    return !connected;
}

int ZmqSink::getPort() {
    return this->port;
}
SinkType ZmqSink::getType() const {
    return ZMQ_SINK;
}

}  // namespace NES
BOOST_CLASS_EXPORT(NES::ZmqSink);
