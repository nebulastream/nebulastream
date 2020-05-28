#include <Network/NetworkSource.hpp>

namespace NES {
namespace Network {

NetworkSource::NetworkSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager,
                             NetworkManager& networkManager,
                             NesPartition nesPartition) : DataSource(schema, bufferManager, queryManager),
                                                          networkManager(networkManager), nesPartition(nesPartition) {
    networkManager.registerSubpartitionConsumer(nesPartition);
}

NetworkSource::~NetworkSource() {
    networkManager.unregisterSubpartitionConsumer(nesPartition);
}

std::optional<TupleBuffer> NetworkSource::receiveData() {
    NES_THROW_RUNTIME_ERROR("NetworkSource: ReceiveData() called, but method is invalid and should not be used.");
}

SourceType NetworkSource::getType() const {
    return NETWORK_SOURCE;
}

const std::string NetworkSource::toString() const {
    return "NetworkSource: " + nesPartition.toString();
}

}// namespace Network
}// namespace NES