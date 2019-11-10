#include "../../include/SourceSink/ZmqSink.hpp"

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

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
BOOST_CLASS_EXPORT_IMPLEMENT(iotdb::ZmqSink);

namespace iotdb {

ZmqSink::ZmqSink()
    : host(""),
      port(0),
      tupleCnt(0),
      connected(false),
      context(zmq::context_t(1)),
      socket(zmq::socket_t(context, ZMQ_PUB)) {
  IOTDB_DEBUG(
      "DEFAULT ZMQSINK  " << this << ": Init ZMQ Sink to " << host << ":" << port)

}

ZmqSink::ZmqSink(const Schema& schema, const std::string& host,
                 const uint16_t port)
    : DataSink(schema),
      host(host),
      port(port),
      tupleCnt(0),
      connected(false),
      context(zmq::context_t(1)),
      socket(zmq::socket_t(context, ZMQ_PUB)) {
  IOTDB_DEBUG(
      "ZMQSINK  " << this << ": Init ZMQ Sink to " << host << ":" << port)

}
ZmqSink::~ZmqSink() {
  bool success = disconnect();
  if (success) {
    IOTDB_DEBUG("ZMQSINK  " << this << ": Destroy ZMQ Sink")
  } else {
    IOTDB_ERROR(
        "ZMQSINK  " << this << ": Destroy ZMQ Sink failed cause it could not be disconnected")
    assert(0);
  }
  IOTDB_DEBUG("ZMQSINK  " << this << ": Destroy ZMQ Sink")

}

bool ZmqSink::writeData(const TupleBufferPtr input_buffer) {
  bool connected = connect();
  if (!connected) {
    IOTDB_DEBUG(
        "ZMQSINK  " << this << ": cannot write buffer " << input_buffer << " because queue is not connected")
    assert(0);
  }

  IOTDB_DEBUG("ZMQSINK  " << this << ": writes buffer " << input_buffer)
  //	size_t usedBufferSize = input_buffer->num_tuples * input_buffer->tuple_size_bytes;
  zmq::message_t msg(input_buffer->getBufferSizeInBytes());
  // TODO: If possible only copy the content not the empty part
  std::memcpy(msg.data(), input_buffer->getBuffer(), input_buffer->getBufferSizeInBytes());
  tupleCnt = input_buffer->getNumberOfTuples();
      zmq::message_t envelope(sizeof(tupleCnt));
      memcpy(envelope.data(), &tupleCnt, sizeof(tupleCnt));

      bool rc_env = socket.send(envelope, ZMQ_SNDMORE);
      bool rc_msg = socket.send(msg);
      sentBuffer++;
      if (!rc_env || !rc_msg) {
        IOTDB_DEBUG("ZMQSINK  " << this << ": send NOT successful")
        BufferManager::instance().releaseBuffer(input_buffer);
        return false;
      } else {
        IOTDB_DEBUG("ZMQSINK  " << this << ": send successful")
        BufferManager::instance().releaseBuffer(input_buffer);

        return true;
      }
    }

    const std::string ZmqSink::toString() const {
      std::stringstream ss;
      ss << "ZMQ_SINK(";
      ss << "SCHEMA(" << schema.toString() << "), ";
      ss << "HOST=" << host << ", ";
      ss << "PORT=" << port << ", ";
      ss << "TupleCnt=\"" << tupleCnt << "\")";
      return ss.str();
    }

    bool ZmqSink::connect() {
      if (!connected) {
        IOTDB_DEBUG(
            "ZMQSINK  " << this << ": not connected yet, try to connect")
        int linger = 0;
        auto address = std::string("tcp://") + host + std::string(":")
            + std::to_string(port);

        try {
          socket.bind(address.c_str());
          socket.setsockopt(ZMQ_LINGER, &linger, sizeof(linger));
          connected = true;
          IOTDB_DEBUG("ZMQSINK  " << this << ": connected")
        } catch (...) {
          connected = false;
          IOTDB_DEBUG("ZMQSINK  " << this << ": NOT connected")
        }
      } else {
        IOTDB_DEBUG("ZMQSINK  " << this << ": already connected")
      }

      return connected;
    }

    void ZmqSink::setup() {
      connect();
    }
    void ZmqSink::shutdown() {
    }

    bool ZmqSink::disconnect() {
      if (connected) {
        IOTDB_DEBUG(
            "ZMQSINK  " << this << ": not disconnected yet, try to disconnect")

        try {
          socket.close();
          connected = false;
          IOTDB_DEBUG("ZMQSINK  " << this << ": disconnected")

        } catch (...) {
          connected = true;
          IOTDB_DEBUG("ZMQSINK  " << this << ": NOT disconnected")
        }
      } else {
        IOTDB_DEBUG("ZMQSINK  " << this << ": is not connected")
      }

      return !connected;
    }

    }  // namespace iotdb
