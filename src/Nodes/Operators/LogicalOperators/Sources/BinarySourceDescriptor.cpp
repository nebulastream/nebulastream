#include <API/Schema.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/BinarySourceDescriptor.hpp>
#include <utility>
namespace NES {

BinarySourceDescriptor::BinarySourceDescriptor(SchemaPtr schema, std::string filePath) : SourceDescriptor(std::move(schema)), filePath(std::move(filePath)) {}

BinarySourceDescriptor::BinarySourceDescriptor(SchemaPtr schema, std::string streamName, std::string filePath) : SourceDescriptor(std::move(schema), std::move(streamName)), filePath(std::move(filePath)) {}

SourceDescriptorPtr BinarySourceDescriptor::create(SchemaPtr schema, std::string filePath) {
    return std::make_shared<BinarySourceDescriptor>(BinarySourceDescriptor(std::move(schema), std::move(filePath)));
}

SourceDescriptorPtr BinarySourceDescriptor::create(SchemaPtr schema, std::string streamName, std::string filePath) {
    return std::make_shared<BinarySourceDescriptor>(BinarySourceDescriptor(std::move(schema), std::move(streamName), std::move(filePath)));
}

const std::string& BinarySourceDescriptor::getFilePath() const {
    return filePath;
}

bool BinarySourceDescriptor::equal(SourceDescriptorPtr other) {
    if (!other->instanceOf<BinarySourceDescriptor>())
        return false;
    auto otherDefaultSource = other->as<BinarySourceDescriptor>();
    return filePath == otherDefaultSource->getFilePath() && getSchema()->equals(otherDefaultSource->getSchema());
}

std::string BinarySourceDescriptor::toString() {
    return "BinarySourceDescriptor(" + filePath + ")";
}
}// namespace NES
