#include "Nodes/Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp"

namespace NES {

SenseSourceDescriptor::SenseSourceDescriptor(SchemaPtr schema, std::string udfs)
    : SourceDescriptor(schema), udfs(udfs) {}

const std::string& SenseSourceDescriptor::getUdfs() const {
    return udfs;
}

SourceDescriptorType SenseSourceDescriptor::getType() {
    return SenseDescriptor;
}

}

