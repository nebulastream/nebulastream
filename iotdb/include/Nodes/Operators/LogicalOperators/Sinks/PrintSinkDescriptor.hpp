#ifndef NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_PRINTSINKDESCRIPTOR_HPP_
#define NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_PRINTSINKDESCRIPTOR_HPP_

#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>

namespace NES {

class PrintSinkDescriptor : public SinkDescriptor {

  public:
    PrintSinkDescriptor(SchemaPtr schema, std::ostream& outputStream);
    SinkDescriptorType getType() override;
    std::ostream& getOutputStream() const;

  private:
    std::ostream& outputStream;
};

typedef std::shared_ptr<PrintSinkDescriptor> PrintSinkDescriptorPtr;

}

#endif //NES_IMPL_NODES_OPERATORS_LOGICALOPERATORS_SINKS_PRINTSINKDESCRIPTOR_HPP_
