#include <API/Schema.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <utility>

namespace NES {

LogicalStreamSourceDescriptor::LogicalStreamSourceDescriptor(std::string streamName)
    : SourceDescriptor(Schema::create(), std::move(streamName)) {}

SourceDescriptorPtr LogicalStreamSourceDescriptor::create(std::string streamName) {
    return std::make_shared<LogicalStreamSourceDescriptor>(LogicalStreamSourceDescriptor(std::move(streamName)));
}

bool LogicalStreamSourceDescriptor::equal(SourceDescriptorPtr other) {
    if (!other->instanceOf<LogicalStreamSourceDescriptor>()) {
        return false;
    }
    auto otherLogicalStreamSource = other->as<LogicalStreamSourceDescriptor>();
    return getStreamName() == otherLogicalStreamSource->getStreamName();
}

std::string LogicalStreamSourceDescriptor::toString() {
    return "LogicalStreamSourceDescriptor()";
}
}// namespace NES