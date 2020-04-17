#include "Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp"

namespace NES {

PrintSinkDescriptor::PrintSinkDescriptor(SchemaPtr schema, std::ostream& outputStream): SinkDescriptor(schema), outputStream(outputStream) {}

SinkDescriptorType PrintSinkDescriptor::getType() {
    return SinkDescriptorType::PrintDescriptor;
}

std::ostream& PrintSinkDescriptor::getOutputStream() const {
    return outputStream;
}

}
