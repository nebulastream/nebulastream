#include <Network/NetworkSource.hpp>
#include <Util/Logger.hpp>
#include <Network/NetworkCommon.hpp>

namespace NES {
namespace Network {

NetworkSource::NetworkSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager,
                             NetworkManager& networkManager,
                             NesPartition nesPartition) : DataSource(schema, bufferManager, queryManager),
                                                          networkManager(networkManager), nesPartition(nesPartition) {
    // nop
}

NetworkSource::~NetworkSource() {
    // nop
    if (networkManager.isPartitionRegistered(nesPartition)) {
        NES_ERROR("Partition is still registered " << nesPartition);
        NES_THROW_RUNTIME_ERROR("NetworkSource: ~NetworkSource() called, but partition still in use.");
    }
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

bool NetworkSource::start() {
    return networkManager.registerSubpartitionConsumer(nesPartition) == 0;
}

bool NetworkSource::stop() {
    // TODO ensure proper termination: what should happen here when we call stop but refCnt has not reached zero?
    return networkManager.unregisterSubpartitionConsumer(nesPartition) == 0;
}

void NetworkSource::runningRoutine(BufferManagerPtr, QueryManagerPtr) {
    NES_THROW_RUNTIME_ERROR("NetworkSource: runningRoutine() called, but method is invalid and should not be used.");
}

}// namespace Network
}// namespace NES