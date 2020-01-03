#include <Network/ForwardSink.hpp>

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

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>

using std::string;

namespace iotdb {

ForwardSink::ForwardSink()
    : host(""),
      port(0),
      tupleCnt(0),
      connected(false),
      context(zmq::context_t(1)),
      socket(zmq::socket_t(context, ZMQ_PUSH)) {
  IOTDB_DEBUG(
      "DEFAULT ForwardSink  " << this << ": Init ZMQ Sink to " << host << ":" << port)
}

ForwardSink::ForwardSink(const Schema &schema, const std::string &host,
                 const uint16_t port)
    : DataSink(schema),
      host(host),
      port(port),
      tupleCnt(0),
      connected(false),
      context(zmq::context_t(1)),
      socket(zmq::socket_t(context, ZMQ_PUSH)) {
  IOTDB_DEBUG(
      "ForwardSink  " << this << ": Init ZMQ Sink to " << host << ":" << port)

}
ForwardSink::~ForwardSink() {
  bool success = disconnect();
  if (success) {
    IOTDB_DEBUG("ForwardSink  " << this << ": Destroy ZMQ Sink")
  } else {
    IOTDB_ERROR(
        "ForwardSink  " << this << ": Destroy ZMQ Sink failed cause it could not be disconnected")
    assert(0);
  }
  IOTDB_DEBUG("ForwardSink  " << this << ": Destroy ZMQ Sink")

}

bool ForwardSink::writeData(const TupleBufferPtr input_buffer) {
  this->getSchema();

  connected = connect();
  if (!connected) {
    IOTDB_DEBUG(
        "ForwardSink  " << this << ": cannot write buffer " << input_buffer << " because queue is not connected")
    assert(0);
  }

  IOTDB_DEBUG("ForwardSink  " << this << ": writes buffer " << input_buffer)
  try {
    //send envelope with schema information
    string schemaStr = this->getSchema().toString();
    zmq::message_t message(schemaStr.size());
    memcpy (message.data(), schemaStr.data(), schemaStr.size());
    bool rcS = socket.send (message, ZMQ_SNDMORE);

    //send actual data
    zmq::message_t msg(input_buffer->getBufferSizeInBytes());
    std::memcpy(msg.data(), input_buffer->getBuffer(), input_buffer->getBufferSizeInBytes());
    tupleCnt = input_buffer->getNumberOfTuples();
    bool rcM = socket.send(msg);

    sentBuffer++;
    if (!rcS || !rcM) {
      IOTDB_DEBUG("ForwardSink  " << this << ": send NOT successful")
      BufferManager::instance().releaseBuffer(input_buffer);
      return false;
    } else {
      IOTDB_DEBUG("ForwardSink  " << this << ": send successful")
      BufferManager::instance().releaseBuffer(input_buffer);
      return true;
    }
  }
  catch (const zmq::error_t &ex) {
    // recv() throws ETERM when the zmq context is destroyed,
    //  as when AsyncZmqListener::Stop() is called
    if (ex.num() != ETERM) {
      IOTDB_FATAL_ERROR("ZMQSOURCE: " << ex.what())
    }
  }
  return false;
}

const std::string ForwardSink::toString() const {
  std::stringstream ss;
  ss << "ZMQ_SINK(";
  ss << "SCHEMA(" << schema.toString() << "), ";
  ss << "HOST=" << host << ", ";
  ss << "PORT=" << port << ", ";
  ss << "TupleCnt=\"" << tupleCnt << "\")";
  return ss.str();
}

bool ForwardSink::connect() {
  if (!connected) {
    try {
      auto address = std::string("tcp://") + host + std::string(":") + std::to_string(port);
      socket.connect(address.c_str());
      connected = true;
    }
    catch (const zmq::error_t &ex) {
      // recv() throws ETERM when the zmq context is destroyed,
      //  as when AsyncZmqListener::Stop() is called
      if (ex.num() != ETERM) {
        IOTDB_FATAL_ERROR("ZMQSOURCE: " << ex.what())
      }
    }
  }
  if (connected) {
    IOTDB_DEBUG("ForwardSink  " << this << ": connected")
  } else {
    IOTDB_DEBUG("ForwardSink  " << this << ": NOT connected")
  }
  return connected;
}

bool ForwardSink::disconnect() {
  if (connected) {
    socket.close();
    connected = false;
  }
  if (!connected) {
    IOTDB_DEBUG("ForwardSink  " << this << ": disconnected")
  } else {
    IOTDB_DEBUG("ForwardSink  " << this << ": NOT disconnected")
  }
  return !connected;
}

int ForwardSink::getPort() {
  return this->port;
}
SinkType ForwardSink::getType() const {
  return ZMQ_SINK;
}

}  // namespace iotdb
BOOST_CLASS_EXPORT(iotdb::ForwardSink);