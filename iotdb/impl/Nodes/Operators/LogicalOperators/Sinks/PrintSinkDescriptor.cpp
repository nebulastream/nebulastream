#include <Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>

namespace NES {

PrintSinkDescriptor::PrintSinkDescriptor(SchemaPtr schema) : SinkDescriptor(schema) {}

SinkDescriptorPtr PrintSinkDescriptor::create(SchemaPtr schema) {
    return std::make_shared<PrintSinkDescriptor>(PrintSinkDescriptor(schema));
}

}// namespace NES
