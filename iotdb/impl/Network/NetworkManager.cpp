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

void NetworkManager::registerSubpartitionConsumer(NesPartition nesPartition) {
    NES_INFO("NetworkManager: Registering SubpartitionConsumer: " << nesPartition.toString());
    partitionManager->registerSubpartition(nesPartition);
}

OutputChannel* NetworkManager::registerSubpartitionProducer(const NodeLocation& nodeLocation, NesPartition nesPartition,
                                                            std::function<void(Messages::ErroMessage)>&& onError,
                                                            u_int64_t waitTime, u_int64_t retryTimes) {
    NES_INFO("NetworkManager: Registering SubpartitionProducer: " << nesPartition.toString());
    // method needs to return a pointer so that it can be passed to boost::thread_specific_ptr
    auto channel = new OutputChannel(server->getContext(), nodeLocation.createZmqURI(), nesPartition, onError);
    if (channel->isReady()) {
        //channel is connected and ready to go
        return channel;
    } else if (retryTimes > 0) {
        // try to reconnect
        int i = 1;
        while (!channel->isReady() && i <= retryTimes) {
            delete channel;
            sleep(waitTime);
            NES_INFO("NetworkManager: Registration of SubpartitionProducer not successful! Reconnecting " << i);
            channel = new OutputChannel(server->getContext(), nodeLocation.createZmqURI(), nesPartition, onError);
            i = i + 1;
        }
    }

    if (channel->isReady()) {
        // if reconnect successful, return
        return channel;
    } else {
        delete channel;
        NES_ERROR("NetworkManager: RegisterSubpartitionProducer failed! Registration not possible.");
        return nullptr;
    }
}

}// namespace Network
}// namespace NES