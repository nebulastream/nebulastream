#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_ZMQSOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_ZMQSOURCEDESCRIPTOR_HPP_

#include <Nodes/Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical zmq source
 */
class ZmqSourceDescriptor : public SourceDescriptor {

  public:
    static SourceDescriptorPtr create(SchemaPtr schema, std::string host, uint16_t port);

    /**
     * @brief Get zmq host name
     */
    const std::string& getHost() const;

    /**
     * @brief Get zmq port number
     */
    uint16_t getPort() const;

  private:
    explicit ZmqSourceDescriptor(SchemaPtr schema, std::string host, uint16_t port);

    std::string host;
    uint16_t port;
};

typedef std::shared_ptr<ZmqSourceDescriptor> ZmqSourceDescriptorPtr;

}// namespace NES

#endif//NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_ZMQSOURCEDESCRIPTOR_HPP_
