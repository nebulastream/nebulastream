#include <Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <utility>

namespace NES {
namespace Network {

NetworkSourceDescriptor::NetworkSourceDescriptor(SchemaPtr schema, NesPartition nesPartition) : SourceDescriptor(std::move(schema)), nesPartition(nesPartition) {
}

SourceDescriptorPtr NetworkSourceDescriptor::create(SchemaPtr schema, NesPartition nesPartition) {
    return std::make_shared<NetworkSourceDescriptor>(NetworkSourceDescriptor(std::move(schema), nesPartition));
}

bool NetworkSourceDescriptor::equal(SourceDescriptorPtr other) {
    if (!other->instanceOf<NetworkSourceDescriptor>()) {
        return false;
    }
    auto otherNetworkSource = other->as<NetworkSourceDescriptor>();
    return schema == otherNetworkSource->schema && nesPartition == otherNetworkSource->nesPartition;
}

std::string NetworkSourceDescriptor::toString() {
    return "NetworkSourceDescriptor()";
}

NesPartition NetworkSourceDescriptor::getNesPartition() const {
    return nesPartition;
}

}// namespace Network
}// namespace NES