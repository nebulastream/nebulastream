#ifndef NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_PRINTSINKDESCRIPTOR_HPP_
#define NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_PRINTSINKDESCRIPTOR_HPP_

#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>

namespace NES {

class PrintSinkDescriptor : public SinkDescriptor {

  public:

    PrintSinkDescriptor() = default;

    SinkDescriptorType getType() override;
};

}

#endif //NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_PRINTSINKDESCRIPTOR_HPP_
