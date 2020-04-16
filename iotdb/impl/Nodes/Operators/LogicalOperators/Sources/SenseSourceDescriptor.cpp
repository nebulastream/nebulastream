#include "Nodes/Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp"

namespace NES {

SenseSourceDescriptor::SenseSourceDescriptor(std::string udfs) : udfs(udfs) {}

const std::string& SenseSourceDescriptor::getUdfs() const {
    return udfs;
}

SourceDescriptorType SenseSourceDescriptor::getType() {
    return SenseDescriptor;
}

}

