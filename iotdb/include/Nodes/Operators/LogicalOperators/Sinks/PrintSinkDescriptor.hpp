#ifndef NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_PRINTSINKDESCRIPTOR_HPP_
#define NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_PRINTSINKDESCRIPTOR_HPP_

#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical print sink
 */
class PrintSinkDescriptor : public SinkDescriptor {

  public:
    PrintSinkDescriptor(SchemaPtr schema, std::ostream& outputStream);
    SinkDescriptorType getType() override;

    /**
     * @brief get the output stream which is used for writing the output
     */
    std::ostream& getOutputStream() const;

  private:
    std::ostream& outputStream;
};

typedef std::shared_ptr<PrintSinkDescriptor> PrintSinkDescriptorPtr;

}

#endif //NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_PRINTSINKDESCRIPTOR_HPP_
