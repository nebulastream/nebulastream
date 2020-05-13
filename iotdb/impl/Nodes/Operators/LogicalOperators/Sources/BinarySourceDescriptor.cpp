#include <Nodes/Operators/LogicalOperators/Sources/BinarySourceDescriptor.hpp>

namespace NES {

BinarySourceDescriptor::BinarySourceDescriptor(SchemaPtr schema, std::string filePath) : SourceDescriptor(schema), filePath(filePath) {}

SourceDescriptorPtr BinarySourceDescriptor::create(SchemaPtr schema, std::string filePath) {
    return std::make_shared<BinarySourceDescriptor>(BinarySourceDescriptor(schema, filePath));
}

const std::string& BinarySourceDescriptor::getFilePath() const {
    return filePath;
}

}// namespace NES
