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

OutputChannel* NetworkManager::registerSubpartitionProducer(const NodeLocation& nodeLocation, NesPartition nesPartition,
                                                            std::function<void(Messages::ErroMessage)>&& onError,
                                                            u_int64_t waitTime, u_int64_t retryTimes) {
    NES_INFO("NetworkManager: Registering SubpartitionProducer: " <<  nesPartition.toString());
    // method needs to return a pointer so that it can be passed to boost::thread_specific_ptr
    auto channel = new OutputChannel(server->getContext(), nodeLocation.createZmqURI(), nesPartition, onError);
    if (channel->isReady() ) {
        //channel is connected and ready to go
        return channel;
    }
    else if (retryTimes>0) {
        // try to reconnect
        int i = 1;
        while (!channel->isReady() && i <= retryTimes) {
            NES_INFO("NetworkManager: Registration of SubpartitionProducer not successful! Reconnecting " << i);
            sleep(waitTime);
            delete channel;
            channel = new OutputChannel(server->getContext(), nodeLocation.createZmqURI(), nesPartition, onError);
            i = i + 1;
        }
    }

    if (channel->isReady()) {
        // if reconnect successful, return
        return channel;
    }
    else {
        delete channel;
        throw std::runtime_error("NetworkManager: RegisterSubpartitionProducer failed! Registration not possible.");
    }
}

}// namespace Network
}// namespace NES