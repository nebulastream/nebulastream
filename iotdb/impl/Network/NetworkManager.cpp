#include <Network/NetworkManager.hpp>
#include <Network/OutputChannel.hpp>
#include <Network/ZmqServer.hpp>
#include <Util/Logger.hpp>

namespace NES {

namespace Network {

NetworkManager::NetworkManager(const std::string& hostname, uint16_t port,
                               std::function<void(uint64_t*, TupleBuffer)>&& onDataBuffer,
                               std::function<void()>&& onEndOfStream, std::function<void(std::exception_ptr)>&& onError,
                               BufferManagerPtr bufferManager, uint16_t numServerThread)
    : exchangeProtocol(std::move(onDataBuffer), std::move(onEndOfStream), std::move(onError)),
      server(std::make_shared<ZmqServer>(hostname, port, numServerThread, exchangeProtocol, bufferManager)) {
    bool success = server->start();
    if (success) {
        NES_INFO("NetworkManager: Server started successfully")
    }
    else {
        NES_FATAL_ERROR("NetworkManager: Server failed to start")
    }
}

void NetworkManager::registerSubpartitionConsumer(QueryId queryId, OperatorId operatorId, PartitionId partitionId,
                                                  SubpartitionId subpartitionId) {
    NES_INFO("[NetworkManager] SubpartitionConsumer registered: " << queryId << "::" << operatorId
                                                                  << "::" << partitionId << "::" << subpartitionId)

}

OutputChannel NetworkManager::registerSubpartitionProducer(const NodeLocation& nodeLocation, QueryId queryId,
                                                           OperatorId operatorId, PartitionId partitionId,
                                                           SubpartitionId subpartitionId) {
    NES_INFO("[NetworkManager] SubpartitionProducer registered: " << queryId << "::" << operatorId
                                                                  << "::" << partitionId << "::" << subpartitionId)
    return OutputChannel{server->getContext(), queryId, operatorId,
                         partitionId, subpartitionId, nodeLocation.createZmqURI()};
}

} // namespace Network
} // namespace NES