#ifndef ZMQSOURCE_HPP
#define ZMQSOURCE_HPP

#include <cstdint>
#include <memory>
#include <string>
#include <zmq.hpp>

#include <Core/TupleBuffer.hpp>
#include <Runtime/DataSource.hpp>

namespace iotdb {

class ZmqSource : public DataSource {

public:
  ZmqSource(const Schema &schema, const std::string host, const uint16_t port, const std::string topic);
  ~ZmqSource();

  TupleBuffer receiveData() override;
  const std::string toString() const override;

private:
  const std::string host;
  const uint16_t port;
  const std::string topic;

  bool connected;
  std::unique_ptr<zmq::context_t> zmq_context;
  std::unique_ptr<zmq::socket_t> zmq_socket;

  bool zmq_connect();
  bool zmq_disconnect();
};

} // namespace iotdb

#endif // ZMQSOURCE_HPP