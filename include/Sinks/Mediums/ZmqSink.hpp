#ifndef ZMQSINK_HPP
#define ZMQSINK_HPP

#include <cstdint>
#include <memory>
#include <string>
#include <zmq.hpp>

#include <Sinks/Mediums/SinkMedium.hpp>

namespace NES {

class ZmqSink : public SinkMedium {

  public:
    //TODO: remove internal flag once the new network stack is in place
    ZmqSink(SinkFormatPtr format, const std::string& host, uint16_t port, bool internal, QuerySubPlanId parentPlanId);
    ~ZmqSink();

    bool writeData(TupleBuffer& input_buffer, WorkerContextRef) override;
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
    std::string toString() override;

    /**
    * @brief method to return the type of medium
    * @return type of medium
    */
    SinkMediumTypes getSinkMediumType();

  private:
    ZmqSink();

    std::string host;
    uint16_t port;

    bool connected;
    zmq::context_t context;
    zmq::socket_t socket;
    bool internal;

    bool connect();
    bool disconnect();
};
typedef std::shared_ptr<ZmqSink> ZmqSinkPtr;
}// namespace NES

#endif// ZMQSINK_HPP
