#include <cassert>
#include <cstdint>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <zmq.hpp>
#include <Util/ErrorHandling.hpp>

#include <Runtime/ZmqSink.hpp>

namespace iotdb {

ZmqSink::ZmqSink(const Schema &schema, const std::string &host, const uint16_t port, const std::string &topic)
    : DataSink(schema), host(host), port(port), topic(topic), connected(false), context(zmq::context_t(1)),
      socket(zmq::socket_t(context, ZMQ_PUB)) {}
ZmqSink::~ZmqSink() { assert(disconnect()); }

bool ZmqSink::writeData(const std::vector<TupleBufferPtr> &input_buffers) {
  assert(connect());

  for (auto &buf : input_buffers) {
	 if(!writeData(buf))
	 {
		 return false;
	 }
  }
  return true;
}

bool ZmqSink::writeData(const TupleBufferPtr input_buffer)
{
	  assert(connect());
	zmq::message_t msg(input_buffer->buffer_size);
	std::memcpy(msg.data(), input_buffer->buffer, input_buffer->buffer_size);

	zmq::message_t envelope(topic.size());
	memcpy(envelope.data(), topic.data(), topic.size());

	bool rc_env = socket.send(envelope, ZMQ_SNDMORE);
	bool rc_msg = socket.send(msg);
	if (!rc_env || !rc_msg)
	{
		  return false;
	}
	else
	{
		return true;
	}
}

const std::string ZmqSink::toString() const {
  std::stringstream ss;
  ss << "ZMQ_SINK(";
  ss << "SCHEMA(" << schema.toString() << "), ";
  ss << "HOST=" << host << ", ";
  ss << "PORT=" << port << ", ";
  ss << "TOPIC=\"" << topic << "\")";
  return ss.str();
}

bool ZmqSink::connect() {
  if (!connected) {
    int linger = 0;
    auto address = std::string("tcp://") + host + std::string(":") + std::to_string(port);

    try {
      socket.bind(address.c_str());
      socket.setsockopt(ZMQ_LINGER, &linger, sizeof(linger));
      connected = true;
    } catch (...) {
      connected = false;
    }
  }
  return connected;
}

bool ZmqSink::disconnect() {
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
