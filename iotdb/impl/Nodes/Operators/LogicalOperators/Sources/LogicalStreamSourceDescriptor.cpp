#include <API/Schema.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>

namespace NES {

LogicalStreamSourceDescriptor::LogicalStreamSourceDescriptor(std::string streamName)
    : SourceDescriptor(Schema::create()), streamName(streamName) {}

const std::string& LogicalStreamSourceDescriptor::getStreamName() const {
    return streamName;
}

SourceDescriptorPtr LogicalStreamSourceDescriptor::create(std::string streamName) {
    return std::make_shared<LogicalStreamSourceDescriptor>(LogicalStreamSourceDescriptor(streamName));
}

bool LogicalStreamSourceDescriptor::equal(SourceDescriptorPtr other) {
    if (!other->instanceOf<LogicalStreamSourceDescriptor>())
        return false;
    auto otherZMQSource = other->as<LogicalStreamSourceDescriptor>();
    return streamName == otherZMQSource->getStreamName();
}
}// namespace NES