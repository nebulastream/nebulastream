#include "../../include/SourceSink/ZmqSource.hpp"

#include <cassert>
#include <cstdint>
#include <cstring>
#include <sstream>
#include <string>
#include <zmq.hpp>

#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/Dispatcher.hpp>

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>

namespace iotdb {

ZmqSource::ZmqSource()
    : host(""),
      port(0),
      connected(false),
      context(zmq::context_t(1)),
      socket(zmq::socket_t(context, ZMQ_SUB)) {
  IOTDB_DEBUG(
      "Default ZMQSOURCE  " << this << ": Init ZMQ ZMQSOURCE to " << host << ":" << port << "/")
}

ZmqSource::ZmqSource(const Schema &schema, const std::string &host,
                     const uint16_t port)
    : DataSource(schema),
      host(host),
      port(port),
      connected(false),
      context(zmq::context_t(1)),
      socket(zmq::socket_t(context, ZMQ_SUB)) {
  IOTDB_DEBUG(
      "ZMQSOURCE  " << this << ": Init ZMQ ZMQSOURCE to " << host << ":" << port << "/")
}
ZmqSource::~ZmqSource() {
  bool success = disconnect();
  if (success) {
    IOTDB_DEBUG("ZMQSOURCE  " << this << ": Destroy ZMQ Source")
  } else {
    IOTDB_ERROR(
        "ZMQSOURCE  " << this << ": Destroy ZMQ Source failed cause it could not be disconnected")
    assert(0);
  }
  IOTDB_DEBUG("ZMQSOURCE  " << this << ": Destroy ZMQ Source")
}

TupleBufferPtr ZmqSource::receiveData() {
  bool connected = connect();
  if (!connected) {
    IOTDB_DEBUG(
        "ZMQSOURCE  " << this << ": cannot read buffer because queue is not connected")
    assert(0);
  }
  IOTDB_DEBUG("ZMQSource  " << this << ": receiveData ")

  // Receive new chunk of data
  zmq::message_t new_data;
  socket.recv(&new_data);  // envelope - not needed at the moment
  size_t tupleCnt = *((size_t *) new_data.data());
  IOTDB_DEBUG("ZMQSource received #tups " << tupleCnt)

  // actual data
  socket.recv(&new_data);

  // Get some information about received data
  size_t tuple_size = schema.getSchemaSize();

  // Create new TupleBuffer and copy data
  TupleBufferPtr buffer = BufferManager::instance().getBuffer();
  IOTDB_DEBUG("ZMQSource  " << this << ": got buffer ")

  // TODO: If possible only copy the content not the empty part
  std::memcpy(buffer->getBuffer(), new_data.data(), buffer->getBufferSizeInBytes());
  buffer->setNumberOfTuples(tupleCnt);
  buffer->setTupleSizeInBytes(tuple_size);

  assert(buffer->getBufferSizeInBytes() == new_data.size());
  IOTDB_DEBUG("ZMQSource  " << this << ": return buffer ")

  return buffer;
}

const std::string ZmqSource::toString() const {
  std::stringstream ss;
  ss << "ZMQ_SOURCE(";
  ss << "SCHEMA(" << schema.toString() << "), ";
  ss << "HOST=" << host << ", ";
  ss << "PORT=" << port << ", ";
  return ss.str();
}

bool ZmqSource::connect() {
  if (!connected) {
    int linger = 0;
    auto address = std::string("tcp://") + host + std::string(":")
        + std::to_string(port);

    try {
      socket.connect(address.c_str());
      //      socket.setsockopt(ZMQ_SUBSCRIBE, topic.c_str(), topic.size());
      socket.setsockopt(ZMQ_SUBSCRIBE, "", 0);
      socket.setsockopt(ZMQ_LINGER, &linger, sizeof(linger));
      connected = true;
    } catch (...) {
      connected = false;
    }
  }
  if (connected) {
    IOTDB_DEBUG("ZMQSOURCE  " << this << ": connected")
  } else {
    IOTDB_DEBUG("ZMQSOURCE  " << this << ": NOT connected")
  }

  return connected;
}

bool ZmqSource::disconnect() {
  if (connected) {

    try {
      socket.close();
      connected = false;
    } catch (...) {
      connected = true;
    }
  }
  if (!connected) {
    IOTDB_DEBUG("ZMQSOURCE  " << this << ": disconnected")
  } else {
    IOTDB_DEBUG("ZMQSOURCE  " << this << ": NOT disconnected")
  }
  return !connected;
}

}  // namespace iotdb

BOOST_CLASS_EXPORT(iotdb::ZmqSource);