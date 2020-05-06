#ifndef ZMQSINK_HPP
#define ZMQSINK_HPP

#include <cstdint>
#include <memory>
#include <string>
#include <zmq.hpp>

#include <SourceSink/DataSink.hpp>

namespace NES {

class ZmqSink : public DataSink {

  public:
    ZmqSink(SchemaPtr schema, const std::string& host, uint16_t port);
    ~ZmqSink() override;

    bool writeData(TupleBuffer& input_buffer);
    void setup() override { connect(); };
    void shutdown() override{};
    const std::string toString() const override;
    int getPort();
    SinkType getType() const override;

  private:
    ZmqSink();

    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar& boost::serialization::base_object<DataSink>(*this);
        ar& host;
        ar& port;
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
}// namespace NES

#endif// ZMQSINK_HPP
