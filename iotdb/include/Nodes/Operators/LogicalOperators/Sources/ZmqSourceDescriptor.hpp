#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_ZMQSOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_ZMQSOURCEDESCRIPTOR_HPP_

#include <Nodes/Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES {

class ZmqSourceDescriptor : public SourceDescriptor {

  public:

    ZmqSourceDescriptor(std::string host, uint16_t port);

    SourceDescriptorType getType() override;

    const std::string& getHost() const;
    uint16_t getPort() const;

  private:
    std::string host;
    uint16_t port;

};

}

#endif //NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_ZMQSOURCEDESCRIPTOR_HPP_
