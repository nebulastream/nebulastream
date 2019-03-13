#ifndef ZMQSINK_HPP
#define ZMQSINK_HPP

#include <cstdint>
#include <memory>
#include <string>
#include <zmq.hpp>

#include <Runtime/DataSink.hpp>

namespace iotdb {

class ZmqSink : public DataSink {

public:
  ZmqSink(const Schema &schema, const std::string &host,
		  const uint16_t port);
  ~ZmqSink();

  bool writeData(const std::vector<TupleBufferPtr> &input_buffers) override;
  bool writeData(const TupleBufferPtr input_buffer) override;
  bool writeData(const std::vector<TupleBuffer*> &input_buffers) override;
  bool writeData(const TupleBuffer* input_buffer);

  void setup(){connect();};
  void shutdown(){};
  const std::string toString() const override;

private:
  const std::string host;
  const uint16_t port;
  size_t tupleCnt;

  bool connected;
  zmq::context_t context;
  zmq::socket_t socket;

  bool connect();
  bool disconnect();
};
} // namespace iotdb

#endif // ZMQSINK_HPP
