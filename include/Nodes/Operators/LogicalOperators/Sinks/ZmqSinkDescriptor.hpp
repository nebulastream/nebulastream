#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SINKS_ZMQSINKDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SINKS_ZMQSINKDESCRIPTOR_HPP_

#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical zmq sink
 */
class ZmqSinkDescriptor : public SinkDescriptor {

  public:
    /**
     * @brief Creates the ZMQ sink description
     * @param host: host name for connecting to zmq
     * @param port: port number for connecting to zmq
     * @param internal: defines if the zmq should send the message schema as a first message
     * @return descriptor for ZMQ sink
     */
    static SinkDescriptorPtr create(std::string host, uint16_t port, bool internal = false);

    /**
     * @brief Get the zmq host where the data is to be written
     */
    const std::string& getHost() const;

    /**
     * @brief Get the zmq port used for connecting to the server
     */
    uint16_t getPort() const;

    /**
     * Set the zmq port information
     * @param port : zmq port number
     */
    void setPort(uint16_t port);

    bool isInternal() const;
    void setInternal(bool internal);

    std::string toString() override;
    bool equal(SinkDescriptorPtr other) override;

  private:
    explicit ZmqSinkDescriptor(std::string host, uint16_t port, bool internal);

    std::string host;
    uint16_t port;
    bool internal;
};

typedef std::shared_ptr<ZmqSinkDescriptor> ZmqSinkDescriptorPtr;

}// namespace NES

#endif//NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SINKS_ZMQSINKDESCRIPTOR_HPP_
