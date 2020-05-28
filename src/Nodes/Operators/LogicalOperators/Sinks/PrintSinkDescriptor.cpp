#include <Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>

namespace NES {

PrintSinkDescriptor::PrintSinkDescriptor() {}

SinkDescriptorPtr PrintSinkDescriptor::create() {
    return std::make_shared<PrintSinkDescriptor>(PrintSinkDescriptor());
}

std::string PrintSinkDescriptor::toString() {
    return "PrintSinkDescriptor()";
}
bool PrintSinkDescriptor::equal(SinkDescriptorPtr other) {
    return other->instanceOf<PrintSinkDescriptor>();
}

}// namespace NES
