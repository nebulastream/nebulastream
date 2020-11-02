#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_ZMQSOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_ZMQSOURCEDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical zmq source
 */
class ZmqSourceDescriptor : public SourceDescriptor {

  public:
    static SourceDescriptorPtr create(SchemaPtr schema, std::string host, uint16_t port);
    static SourceDescriptorPtr create(SchemaPtr schema, std::string streamName, std::string host, uint16_t port);

    /**
     * @brief Get zmq host name
     */
    const std::string& getHost() const;

    /**
     * @brief Get zmq port number
     */
    uint16_t getPort() const;

    /**
     * Set the zmq port information
     * @param port : zmq port number
     */
    void setPort(uint16_t port);

    bool equal(SourceDescriptorPtr other) override;
    std::string toString() override;

  private:
    explicit ZmqSourceDescriptor(SchemaPtr schema, std::string host, uint16_t port);
    explicit ZmqSourceDescriptor(SchemaPtr schema, std::string streamName, std::string host, uint16_t port);

    std::string host;
    uint16_t port;
};

typedef std::shared_ptr<ZmqSourceDescriptor> ZmqSourceDescriptorPtr;

}// namespace NES

#endif//NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_ZMQSOURCEDESCRIPTOR_HPP_
