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
    ZmqSink(const Schema &schema, const std::string host, const uint16_t port, const std::string topic);
    ~ZmqSink();

    bool writeData(const TupleBuffer& buf) override;
    const std::string toString() const override;

private:
    const std::string host;
    const uint16_t port;
    const std::string topic;

    bool connected;
    zmq::context_t context;
    zmq::socket_t socket;

    bool connect();
    bool disconnect();
};
}

#endif //IOTDB_ZMQSINK_HPP
