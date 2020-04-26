#ifndef NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_PRINTSINKDESCRIPTOR_HPP_
#define NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_PRINTSINKDESCRIPTOR_HPP_

#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical print sink
 */
class PrintSinkDescriptor : public SinkDescriptor {

  public:
    /**
     * @brief Factory method to create a new prink sink descriptor
     * @param schema
     * @return
     */
    static SinkDescriptorPtr create(SchemaPtr schema);
    SinkDescriptorType getType() override;
  private:
    PrintSinkDescriptor(SchemaPtr schema);
};

typedef std::shared_ptr<PrintSinkDescriptor> PrintSinkDescriptorPtr;

}

#endif //NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_PRINTSINKDESCRIPTOR_HPP_
