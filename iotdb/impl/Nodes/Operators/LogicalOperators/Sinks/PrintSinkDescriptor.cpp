#include <Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>

namespace NES {

PrintSinkDescriptor::PrintSinkDescriptor() {}

SinkDescriptorPtr PrintSinkDescriptor::create() {
    return std::make_shared<PrintSinkDescriptor>(PrintSinkDescriptor());
}

}// namespace NES
