#ifndef IOTDB_INCLUDE_NETWORK_FORWARDSINK_HPP_
#define IOTDB_INCLUDE_NETWORK_FORWARDSINK_HPP_

#include <SourceSink/DataSink.hpp>

#include <cstdint>
#include <memory>
#include <string>
#include <zmq.hpp>

namespace iotdb {

class ForwardSink : public DataSink {
 public:
  ForwardSink(const Schema &schema, const std::string &host, uint16_t port);
  ~ForwardSink() override;

  bool writeData(TupleBufferPtr input_buffer);
  void setup() override { connect(); };
  void shutdown() override {};
  const std::string toString() const override;
  int getPort();
  SinkType getType() const override;
 private:
  ForwardSink();

  friend class boost::serialization::access;
  template<class Archive>
  void serialize(Archive &ar, const unsigned int version) {
    ar & boost::serialization::base_object<DataSink>(*this);
    ar & host;
    ar & port;
  }

  std::string host;
  uint16_t port;
  size_t tupleCnt;

  bool connected;
  zmq::context_t context;
  zmq::socket_t socket;

  bool connect();
  bool disconnect();
};
} // namespace iotdb

#endif //IOTDB_INCLUDE_NETWORK_FORWARDSINK_HPP_
