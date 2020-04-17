#include "Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp"

namespace NES {

PrintSinkDescriptor::PrintSinkDescriptor(): SinkDescriptor() {}

SinkDescriptorType PrintSinkDescriptor::getType() {
    return SinkDescriptorType::PrintDescriptor;
}

}
