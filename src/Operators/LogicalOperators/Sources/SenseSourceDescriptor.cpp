#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>
#include <utility>
namespace NES {

SenseSourceDescriptor::SenseSourceDescriptor(SchemaPtr schema, std::string udfs)
    : SourceDescriptor(std::move(schema)), udfs(std::move(udfs)) {}

SenseSourceDescriptor::SenseSourceDescriptor(SchemaPtr schema, std::string streamName, std::string udfs)
    : SourceDescriptor(std::move(schema), std::move(streamName)), udfs(std::move(udfs)) {}

const std::string& SenseSourceDescriptor::getUdfs() const {
    return udfs;
}

SourceDescriptorPtr SenseSourceDescriptor::create(SchemaPtr schema, std::string streamName, std::string udfs) {
    return std::make_shared<SenseSourceDescriptor>(SenseSourceDescriptor(std::move(schema), std::move(streamName), std::move(udfs)));
}

SourceDescriptorPtr SenseSourceDescriptor::create(SchemaPtr schema, std::string udfs) {
    return std::make_shared<SenseSourceDescriptor>(SenseSourceDescriptor(std::move(schema), std::move(udfs)));
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
