#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SINKS_ZMQSINKDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SINKS_ZMQSINKDESCRIPTOR_HPP_

#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>

namespace NES {

class ZmqSinkDescriptor : public SinkDescriptor {

  public:

    ZmqSinkDescriptor(std::string host, uint16_t port, size_t tupleCnt);

    SinkDescriptorType getType() override;

    const std::string& getHost() const;
    uint16_t getPort() const;
    size_t getTupleCnt() const;

  private:

    ZmqSinkDescriptor() = default;

    std::string host;
    uint16_t port;
    size_t tupleCnt;
};

}

#endif //NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SINKS_ZMQSINKDESCRIPTOR_HPP_
