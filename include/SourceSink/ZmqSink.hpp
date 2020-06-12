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

    bool writeData(TupleBuffer& input_buffer) override;
    void setup() override { connect(); };
    void shutdown() override{};
    const std::string toString() const override;

    /**
     * @brief Get zmq sink port
     */
    int getPort();

    /**
     * @brief Get Zmq host name
     */
    const std::string getHost() const;

    /**
     * @brief Get Sink type
     */
    SinkType getType() const override;

  private:
    ZmqSink();

    std::string host;
    uint16_t port;
    size_t tupleCnt;

    bool connected;
    zmq::context_t context;
    zmq::socket_t socket;

    bool connect();
    bool disconnect();
};
typedef std::shared_ptr<ZmqSink> ZmqSinkPtr;
}// namespace NES

#endif// ZMQSINK_HPP
