#include <Runtime/ZmqSource.hpp>

namespace iotdb {

ZmqSource::ZmqSource(const Schema &schema, const std::string host, const uint16_t port, const std::string topic)
    : DataSource(schema), host(host), port(port), topic(topic) {
  // TODO
}
ZmqSource::~ZmqSource() {
  // TODO
}

TupleBuffer ZmqSource::receiveData() {
  // TODO
}

const std::string ZmqSource::toString() const {
    // TODO
};
} // namespace iotdb
