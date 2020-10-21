#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <utility>
namespace NES {

SourceDescriptor::SourceDescriptor(SchemaPtr schema) : schema(std::move(schema)), streamName() {}

SourceDescriptor::SourceDescriptor(SchemaPtr schema, std::string streamName) : schema(std::move(schema)), streamName(std::move(streamName)) {}

SchemaPtr SourceDescriptor::getSchema() {
    return schema;
}

std::string SourceDescriptor::getStreamName() {
    return streamName;
}

bool SourceDescriptor::hasStreamName() {
    return !streamName.empty();
}

void SourceDescriptor::setSchema(SchemaPtr schema) {
    this->schema = schema;
}

}// namespace NES
