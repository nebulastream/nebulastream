#include <Network/NetworkManager.hpp>
#include <Network/OutputChannel.hpp>
#include <Network/ZmqServer.hpp>
#include <Network/PartitionManager.hpp>
#include <Util/Logger.hpp>

namespace NES {

namespace Network {

NetworkManager::NetworkManager(const std::string& hostname,
                               uint16_t port,
                               std::function<void(uint64_t*, TupleBuffer)>&& onDataBuffer,
                               std::function<void(NesPartition)>&& onEndOfStream,
                               std::function<void(Messages::ErroMessage)>&& onError,
                               BufferManagerPtr bufferManager,
                               PartitionManagerPtr partitionManager,
                               uint16_t numServerThread)
    : server(std::make_shared<ZmqServer>(hostname, port, numServerThread, exchangeProtocol, bufferManager,
                                         partitionManager)),
      exchangeProtocol(std::move(onDataBuffer), std::move(onEndOfStream), std::move(onError)),
      partitionManager(partitionManager) {
    bool success = server->start();
    if (success) {
        NES_INFO("NetworkManager: Server started successfully");
    } else {
        NES_FATAL_ERROR("NetworkManager: Server failed to start");
    }
}

void NetworkManager::registerSubpartitionConsumer(QueryId queryId, OperatorId operatorId, PartitionId partitionId,
                                                  SubpartitionId subpartitionId) {
    NES_INFO("NetworkManager: Registering SubpartitionConsumer: " << queryId << "::" << operatorId
                                                                  << "::" << partitionId << "::" << subpartitionId);
    partitionManager->registerSubpartition(NesPartition(queryId, operatorId, partitionId, subpartitionId));
}

OutputChannel* NetworkManager::registerSubpartitionProducer(const NodeLocation& nodeLocation, QueryId queryId,
                                                            OperatorId operatorId, PartitionId partitionId,
                                                            SubpartitionId subpartitionId,
                                                            std::function<void(Messages::ErroMessage)>&& onError) {
    NES_INFO("NetworkManager: Registering SubpartitionProducer: " << queryId << "::" << operatorId
                                                                  << "::" << partitionId << "::" << subpartitionId);
    return new OutputChannel(server->getContext(), queryId, operatorId, partitionId, subpartitionId,
                             nodeLocation.createZmqURI(), std::move(onError));
}

}// namespace Network
}// namespace NES