#include <cassert>
#include <cstdint>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <zmq.hpp>

#include <Core/TupleBuffer.hpp>
#include <Runtime/DataSource.hpp>
#include <Runtime/Dispatcher.hpp>
#include <Runtime/ZmqSource.hpp>

namespace iotdb {

ZmqSource::ZmqSource(const Schema &schema, const std::string &host, const uint16_t port, const std::string &topic)
    : DataSource(schema), host(host), port(port), topic(topic), connected(false), context(zmq::context_t(1)),
      socket(zmq::socket_t(context, ZMQ_SUB)) {}
ZmqSource::~ZmqSource() { assert(disconnect()); }

TupleBuffer ZmqSource::receiveData() {
  assert(connect());

  // Receive new chunk of data
  zmq::message_t new_data;
  socket.recv(&new_data); // envelope - not needed at the moment
  socket.recv(&new_data); // actual data

  // Get some information about received data
  auto buffer_size = new_data.size();
  auto tuple_size = schema.getSchemaSize();
  auto number_of_tuples = buffer_size / tuple_size;

  // Create new TupleBuffer and copy data
  auto buffer = Dispatcher::instance().getBuffer(number_of_tuples);
  assert(buffer.buffer_size == buffer_size);
  std::memcpy(buffer.buffer, new_data.data(), buffer_size);

  return buffer;
}

const std::string ZmqSource::toString() const {
  std::stringstream ss;
  ss << "ZMQ_SOURCE(";
  ss << "SCHEMA(" << schema.toString() << "), ";
  ss << "HOST=" << host << ", ";
  ss << "PORT=" << port << ", ";
  ss << "TOPIC=\"" << topic << "\")";
  return ss.str();
};

bool ZmqSource::connect() {
  if (!connected) {
    int linger = 0;
    auto address = std::string("tcp://") + host + std::string(":") + std::to_string(port);

    try {
      socket.connect(address.c_str());
      socket.setsockopt(ZMQ_SUBSCRIBE, topic.c_str(), topic.size());
      socket.setsockopt(ZMQ_LINGER, &linger, sizeof(linger));
      connected = true;
    } catch (...) {
      connected = false;
    }
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
  return !connected;
}

} // namespace iotdb
