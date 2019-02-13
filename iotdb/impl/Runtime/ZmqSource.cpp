#include <cassert>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <zmq.hpp>

#include <Core/TupleBuffer.hpp>
#include <Runtime/DataSource.hpp>
#include <Runtime/ZmqSource.hpp>

namespace iotdb {

ZmqSource::ZmqSource(const Schema &schema, const std::string host, const uint16_t port, const std::string topic)
    : DataSource(schema), host(host), port(port), topic(topic) {
  connected = false;
  zmq_context = std::make_unique<zmq::context_t>(1);
  zmq_socket = std::make_unique<zmq::socket_t>(zmq_context, ZMQ_SUB);
}

ZmqSource::~ZmqSource() { assert(zmq_disconnect()); }

TupleBuffer ZmqSource::receiveData() {
  assert(zmq_connect());

  // Receive new chunk of data
  zmq_message_t new_data;
  zmq_socket.recv(&new_data);

  // Get some information about received data
  auto buffer_size = new_date.size();
  auto tuple_size = schema.getSchemaSize();
  auto number_of_tuples = buffer_size / tuple_size;

  // Create new TupleBuffer and copy data
  auto buffer = Dispatcher::instance().getBuffer(number_of_tuples);
  assert(buffer.buffer_size == buffer_size);
  std::memcpy(buffer.buffer, &new_data.data(), buffer_size);

  return buffer;
}

const std::string ZmqSource::toString() const {
  std::stringstream ss;
  ss << "ZMQ_SOURCE(";
  ss << "SCHEMA(" << schema.toString() << "), ";
  ss << "HOST=" << host << ", ";
  ss << "PORT=" << port << ", ";
  ss << "TOPIC=" << topic << ")";
  return ss.str();
};

bool ZmqSource::zmq_connect() {
  if (!connected) {
    int fail = 0;
    auto adress = std::string("tcp://") + host + std::string(":") + port;
    fail += zmq_socket.connect(adress);
    fail += zmq_socket.setsockopt(ZMQ_LINGER, 0); // discard all messages after closing the socket
    fail += zmq_socket.setsockopt(ZMQ_SUBSCRIBE, topic.c_str(), topic.size()); // subscribe to given topic
    connected = (fail == 0);
  }
  return connected;
}

bool ZmqSource::zmq_disconnect() {
  if (connected) {
    int fail = 0;
    fail += zmq_socket.close();
    fail += zmq_context.close();
    connected = !(fail == 0);
  }
  return connected;
}

} // namespace iotdb
