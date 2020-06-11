#include <Nodes/Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>

namespace NES {
namespace Network {

NetworkSourceDescriptor::NetworkSourceDescriptor(SchemaPtr schema, NesPartition nesPartition) :
    SourceDescriptor(schema), nesPartition(nesPartition) {
}

SourceDescriptorPtr NetworkSourceDescriptor::create(SchemaPtr schema, NesPartition nesPartition) {
    return std::make_shared<NetworkSourceDescriptor>(NetworkSourceDescriptor(schema, nesPartition));
}

bool NetworkSourceDescriptor::equal(SourceDescriptorPtr other) {
    if (!other->instanceOf<NetworkSourceDescriptor>())
        return false;
    auto outherNetworkSource = other->as<NetworkSourceDescriptor>();
    return schema == outherNetworkSource->schema && nesPartition == outherNetworkSource->nesPartition;
}

std::string NetworkSourceDescriptor::toString() {
    return "NetworkSourceDescriptor()";
}

NesPartition NetworkSourceDescriptor::getNesPartition() const {
    return nesPartition;
}

}
}