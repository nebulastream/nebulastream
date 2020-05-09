#include <API/Schema.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>

namespace NES {

LogicalStreamSourceDescriptor::LogicalStreamSourceDescriptor(std::string streamName)
    : SourceDescriptor(Schema::create()), streamName(streamName) {}

const std::string& LogicalStreamSourceDescriptor::getStreamName() const {
    return streamName;
}
SourceDescriptorType LogicalStreamSourceDescriptor::getType() {
    return LogicalStreamSrc;
}
SourceDescriptorPtr LogicalStreamSourceDescriptor::create(std::string streamName) {
    return std::make_shared<LogicalStreamSourceDescriptor>(LogicalStreamSourceDescriptor(streamName));
}
}// namespace NES