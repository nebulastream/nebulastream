#include <Network/NetworkSource.hpp>

namespace NES {
namespace Network {

NetworkSource::NetworkSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager,
                             NetworkManager& networkManager, const NodeLocation& nodeLocation,
                             NesPartition nesPartition) : DataSource(schema, bufferManager, queryManager),
                                                          networkManager(networkManager), nodeLocation(nodeLocation), nesPartition(nesPartition) {
    networkManager.registerSubpartitionConsumer(nesPartition);
}

NetworkSource::~NetworkSource() {
}

std::optional<TupleBuffer> NetworkSource::receiveData() {
    return std::optional<TupleBuffer>();
}

SourceType NetworkSource::getType() const {
    return NETWORK_SOURCE;
}

const std::string NetworkSource::toString() const {
    return "NetworkSource: " + nesPartition.toString();
}

}// namespace Network
}// namespace NES