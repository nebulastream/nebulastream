#include <Nodes/Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>

namespace NES {

SenseSourceDescriptor::SenseSourceDescriptor(SchemaPtr schema, std::string udfs)
    : SourceDescriptor(schema), udfs(udfs) {}

const std::string& SenseSourceDescriptor::getUdfs() const {
    return udfs;
}

SourceDescriptorType SenseSourceDescriptor::getType() {
    return SenseSrc;
}
SourceDescriptorPtr SenseSourceDescriptor::create(SchemaPtr schema, std::string udfs) {
    return std::make_shared<SenseSourceDescriptor>(SenseSourceDescriptor(schema, udfs));
}

}// namespace NES
