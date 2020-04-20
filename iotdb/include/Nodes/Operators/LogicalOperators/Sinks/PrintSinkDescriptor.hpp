#ifndef NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_PRINTSINKDESCRIPTOR_HPP_
#define NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_PRINTSINKDESCRIPTOR_HPP_

#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical print sink
 */
class PrintSinkDescriptor : public SinkDescriptor {

  public:
    PrintSinkDescriptor(SchemaPtr schema);
    SinkDescriptorType getType() override;
};

typedef std::shared_ptr<PrintSinkDescriptor> PrintSinkDescriptorPtr;

}

#endif //NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_PRINTSINKDESCRIPTOR_HPP_
