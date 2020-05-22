#include <Network/NetworkManager.hpp>
#include <Network/OutputChannel.hpp>
#include <Network/PartitionManager.hpp>
#include <Network/ZmqServer.hpp>
#include <Util/Logger.hpp>

namespace NES {

namespace Network {

NetworkManager::NetworkManager(const std::string& hostname,
                               uint16_t port,
                               std::function<void(NesPartition, TupleBuffer&)>&& onDataBuffer,
                               std::function<void(Messages::EndOfStreamMessage)>&& onEndOfStream,
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

uint64_t NetworkManager::registerSubpartitionConsumer(NesPartition nesPartition) {
    NES_INFO("NetworkManager: Registering SubpartitionConsumer: " << nesPartition.toString());
    return partitionManager->registerSubpartition(nesPartition);
}

OutputChannel* NetworkManager::registerSubpartitionProducer(const NodeLocation& nodeLocation, NesPartition nesPartition,
                                                            std::function<void(Messages::ErroMessage)>&& onError,
                                                            u_int8_t waitTime, u_int8_t retryTimes) {
    NES_INFO("NetworkManager: Registering SubpartitionProducer: " << nesPartition.toString());
    // method needs to return a pointer so that it can be passed to boost::thread_specific_ptr
    auto channel = new OutputChannel(server->getContext(), nodeLocation.createZmqURI(), nesPartition, waitTime, retryTimes, onError);

    if (channel->isConnected()) {
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