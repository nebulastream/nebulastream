#include <Network/NetworkManager.hpp>
#include <Network/OutputChannel.hpp>
#include <Network/PartitionManager.hpp>
#include <Network/ZmqServer.hpp>
#include <Util/Logger.hpp>

namespace NES {

namespace Network {

NetworkManager::NetworkManager(const std::string& hostname, uint16_t port, ExchangeProtocolPtr exchangeProtocol,
                               uint16_t numServerThread)
    : server(std::make_shared<ZmqServer>(hostname, port, numServerThread, exchangeProtocol)),
      exchangeProtocol(exchangeProtocol) {
    bool success = server->start();
    if (success) {
        NES_INFO("NetworkManager: Server started successfully");
    } else {
        NES_THROW_RUNTIME_ERROR("NetworkManager: Server failed to start");
    }
}

NetworkManagerPtr NetworkManager::create(const std::string& hostname,
                                         uint16_t port,
                                         ExchangeProtocolPtr exchangeProtocol,
                                         uint16_t numServerThread) {
    return std::make_shared<NetworkManager>(NetworkManager{hostname, port, exchangeProtocol, numServerThread});
}

bool NetworkManager::isPartitionRegistered(NesPartition nesPartition) const {
    return exchangeProtocol->getPartitionManager()->isRegistered(nesPartition);
}

uint64_t NetworkManager::registerSubpartitionConsumer(NesPartition nesPartition) {
    NES_INFO("NetworkManager: Registering SubpartitionConsumer: " << nesPartition.toString());
    return exchangeProtocol->getPartitionManager()->registerSubpartition(nesPartition);
}

uint64_t NetworkManager::unregisterSubpartitionConsumer(NesPartition nesPartition) {
    NES_INFO("NetworkManager: Unregistering SubpartitionConsumer: " << nesPartition.toString());
    return exchangeProtocol->getPartitionManager()->unregisterSubpartition(nesPartition);
}

OutputChannel* NetworkManager::registerSubpartitionProducer(const NodeLocation& nodeLocation, NesPartition nesPartition,
                                                            std::function<void(Messages::ErroMessage)>&& onError,
                                                            std::chrono::seconds waitTime, uint8_t retryTimes) {
    NES_INFO("NetworkManager: Registering SubpartitionProducer: " << nesPartition.toString());
    // method needs to return a pointer so that it can be passed to boost::thread_specific_ptr
    auto channel = new OutputChannel{server->getContext(), nodeLocation.createZmqURI(), nesPartition, waitTime, retryTimes, onError};

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