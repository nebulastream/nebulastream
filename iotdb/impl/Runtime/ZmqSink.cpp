#include <cassert>
#include <cstdint>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <zmq.hpp>

#include <NodeEngine/Dispatcher.hpp>
#include <Util/ErrorHandling.hpp>
#include <Util/Logger.hpp>

#include <Util/ErrorHandling.hpp>
#include <Util/Logger.hpp>

#include <Runtime/ZmqSink.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
BOOST_CLASS_EXPORT_IMPLEMENT(iotdb::ZmqSink);

namespace iotdb {

ZmqSink::ZmqSink()
    : host(""), port(0), tupleCnt(0), connected(false), context(zmq::context_t(1)),
      socket(zmq::socket_t(context, ZMQ_PUB)){
          IOTDB_DEBUG("DEFAULT ZMQSINK  " << this << ": Init ZMQ Sink to " << host << ":" << port)

      }

      ZmqSink::ZmqSink(const Schema& schema, const std::string& host, const uint16_t port)
    : DataSink(schema), host(host), port(port), tupleCnt(0), connected(false), context(zmq::context_t(1)),
      socket(zmq::socket_t(context, ZMQ_PUB)){
          IOTDB_DEBUG("ZMQSINK  " << this << ": Init ZMQ Sink to " << host << ":" << port)

      } ZmqSink::~ZmqSink()
{
    assert(disconnect());
    IOTDB_DEBUG("ZMQSINK  " << this << ": Destroy ZMQ Source")
}

bool ZmqSink::writeData(const TupleBufferPtr input_buffer)
{
    assert(connect());
    IOTDB_DEBUG("ZMQSINK  " << this << ": writes buffer " << input_buffer)
    //	size_t usedBufferSize = input_buffer->num_tuples * input_buffer->tuple_size_bytes;
    zmq::message_t msg(input_buffer->buffer_size);
    // TODO: If possible only copy the content not the empty part
    std::memcpy(msg.data(), input_buffer->buffer, input_buffer->buffer_size);
    tupleCnt = input_buffer.get()->num_tuples;
    zmq::message_t envelope(sizeof(tupleCnt));
    memcpy(envelope.data(), &tupleCnt, sizeof(tupleCnt));

    bool rc_env = socket.send(envelope, ZMQ_SNDMORE);
    bool rc_msg = socket.send(msg);
    processedBuffer++;
    if (!rc_env || !rc_msg) {
        IOTDB_DEBUG("ZMQSINK  " << this << ": send NOT successful")
//       TODO: here we use tuple Buffer instead of TupleBuffer Pointer, FIXME
        BufferManager::instance().releaseBuffer(input_buffer);
        return false;
    }
    else {
        IOTDB_DEBUG("ZMQSINK  " << this << ": send successful")
    //       TODO: here we use tuple Buffer instead of TupleBuffer Pointer, FIXME
               BufferManager::instance().releaseBuffer(input_buffer);
//        BufferManager::instance().releaseBuffer(input_buffer);

        return true;
    }
}

const std::string ZmqSink::toString() const
{
    std::stringstream ss;
    ss << "ZMQ_SINK(";
    ss << "SCHEMA(" << schema.toString() << "), ";
    ss << "HOST=" << host << ", ";
    ss << "PORT=" << port << ", ";
    ss << "TupleCnt=\"" << tupleCnt << "\")";
    return ss.str();
}

bool ZmqSink::connect()
{
    if (!connected) {
        int linger = 0;
        auto address = std::string("tcp://") + host + std::string(":") + std::to_string(port);

        try {
            socket.bind(address.c_str());
            //      socket.setsockopt(ZMQ_SUBSCRIBE, topic.c_str(), topic.size());
            socket.setsockopt(ZMQ_LINGER, &linger, sizeof(linger));
            connected = true;
        }
        catch (...) {
            connected = false;
        }
    }
    if (connected) {
        IOTDB_DEBUG("ZMQSINK  " << this << ": connected")
    }
    else {
        IOTDB_DEBUG("ZMQSINK  " << this << ": NOT connected")
    }

    return connected;
}

bool ZmqSink::disconnect()
{
    if (connected) {

        try {
            socket.close();
            connected = false;
        }
        catch (...) {
            connected = true;
        }
    }
    if (!connected) {
        IOTDB_DEBUG("ZMQSINK  " << this << ": disconnected")
    }
    else {
        IOTDB_DEBUG("ZMQSINK  " << this << ": NOT disconnected")
    }
    return !connected;
}

} // namespace iotdb
