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
  ZmqSource(const Schema &schema, const std::string &host, const uint16_t port);
  ~ZmqSource();

  TupleBufferPtr receiveData() override;
  const std::string toString() const override;

private:
  const std::string host;
  const uint16_t port;
  bool connected;
  zmq::context_t context;
  zmq::socket_t socket;

  bool connect();
  bool disconnect();
};

} // namespace iotdb

#endif // ZMQSOURCE_HPP
