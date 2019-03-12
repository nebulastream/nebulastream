#include <cassert>
#include <cstdint>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <zmq.hpp>
#include <Util/ErrorHandling.hpp>
#include <Util/Logger.hpp>

#include <Runtime/ZmqSink.hpp>

namespace iotdb {

ZmqSink::ZmqSink(const Schema &schema, const std::string &host, const uint16_t port, const std::string &topic)
    : DataSink(schema), host(host), port(port), topic(topic), connected(false), context(zmq::context_t(1)),
      socket(zmq::socket_t(context, ZMQ_PUB)) {
	  IOTDB_DEBUG("ZMQSINK  " << this << ": Init ZMQ Sink to " << host << ":" << port << "/" << topic)

}
ZmqSink::~ZmqSink() { assert(disconnect());
IOTDB_DEBUG("ZMQSINK  " << this << ": Destroy ZMQ Source")
}

bool ZmqSink::writeData(const std::vector<TupleBufferPtr> &input_buffers) {
  assert(connect());
  assert(0);
  for (auto &buf : input_buffers) {
	 if(!writeData(buf))
	 {
		 return false;
	 }
  }
  return true;
}

bool ZmqSink::writeData(const std::vector<TupleBuffer*> &input_buffers)
{
	assert(0);
  assert(connect());
  for (auto &buf : input_buffers) {
	 if(!writeData(buf))
	 {
		 return false;
	 }
  }
  return true;
}

bool ZmqSink::writeData(const TupleBuffer* input_buffer)
{
	assert(connect());
	IOTDB_DEBUG("ZMQSINK  " << this << ": writes buffer " << input_buffer)

	zmq::message_t msg(input_buffer->buffer_size);
	std::memcpy(msg.data(), input_buffer->buffer, input_buffer->buffer_size);

	zmq::message_t envelope(topic.size());
	memcpy(envelope.data(), topic.data(), topic.size());

	bool rc_env = socket.send(envelope, ZMQ_SNDMORE);
	bool rc_msg = socket.send(msg);
	processedBuffer++;
	if (!rc_env || !rc_msg)
	{
		  IOTDB_DEBUG("ZMQSINK  " << this << ": send NOT successful")
		  return false;
	}
	else
	{
		IOTDB_DEBUG("ZMQSINK  " << this << ": send successful")
		return true;
	}
}
bool ZmqSink::writeData(const TupleBufferPtr input_buffer)
{
	assert(connect());
	IOTDB_DEBUG("ZMQSINK  " << this << ": writes buffer " << input_buffer)

	zmq::message_t msg(input_buffer->buffer_size);
	std::memcpy(msg.data(), input_buffer->buffer, input_buffer->buffer_size);

	zmq::message_t envelope(topic.size());
	memcpy(envelope.data(), topic.data(), topic.size());

	bool rc_env = socket.send(envelope, ZMQ_SNDMORE);
	bool rc_msg = socket.send(msg);
	processedBuffer++;
	if (!rc_env || !rc_msg)
	{
		  IOTDB_DEBUG("ZMQSINK  " << this << ": send NOT successful")
		  return false;
 	}
	else
	{
		IOTDB_DEBUG("ZMQSINK  " << this << ": send successful")
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
  if(connected)
    {
  	  IOTDB_DEBUG("ZMQSINK  " << this << ": connected")
    }
    else
    {
  	  IOTDB_DEBUG("ZMQSINK  " << this << ": NOT connected")
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
  if(!connected)
  {
	  IOTDB_DEBUG("ZMQSINK  " << this << ": disconnected")
  }
  else
  {
	  IOTDB_DEBUG("ZMQSINK  " << this << ": NOT disconnected")
  }
  return !connected;
}

} // namespace iotdb
