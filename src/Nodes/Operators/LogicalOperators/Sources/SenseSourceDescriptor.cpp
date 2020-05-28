#include <API/Schema.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>
namespace NES {

SenseSourceDescriptor::SenseSourceDescriptor(SchemaPtr schema, std::string udfs)
    : SourceDescriptor(schema), udfs(udfs) {}

const std::string& SenseSourceDescriptor::getUdfs() const {
    return udfs;
}

SourceDescriptorPtr SenseSourceDescriptor::create(SchemaPtr schema, std::string udfs) {
    return std::make_shared<SenseSourceDescriptor>(SenseSourceDescriptor(schema, udfs));
}

bool SenseSourceDescriptor::equal(SourceDescriptorPtr other) {
    if (!other->instanceOf<SenseSourceDescriptor>())
        return false;
    auto otherSource = other->as<SenseSourceDescriptor>();
    return udfs == otherSource->getUdfs() && getSchema()->equals(otherSource->getSchema());
}

std::string SenseSourceDescriptor::toString() {
    return "SenseSourceDescriptor()";
}

}// namespace NES
