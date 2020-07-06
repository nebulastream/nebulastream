#include <Network/NetworkManager.hpp>
#include <Network/OutputChannel.hpp>
#include <Network/PartitionManager.hpp>
#include <Network/ZmqServer.hpp>
#include <Util/Logger.hpp>

namespace NES {

namespace Network {

NetworkManager::NetworkManager(const std::string& hostname, uint16_t port, ExchangeProtocol&& exchangeProtocol, BufferManagerPtr bufferManager,
                               uint16_t numServerThread)
    : exchangeProtocol(std::move(exchangeProtocol)),
      server(std::make_shared<ZmqServer>(hostname, port, numServerThread, this->exchangeProtocol, bufferManager)) {
    bool success = server->start();
    if (success) {
        NES_INFO("NetworkManager: Server started successfully");
    } else {
        NES_THROW_RUNTIME_ERROR("NetworkManager: Server failed to start");
    }
}

NetworkManagerPtr NetworkManager::create(const std::string& hostname,
                                         uint16_t port,
                                         Network::ExchangeProtocol&& exchangeProtocol,
                                         BufferManagerPtr bufferManager,
                                         uint16_t numServerThread) {
    return std::make_shared<NetworkManager>(hostname, port, std::move(exchangeProtocol), bufferManager, numServerThread);
}

bool NetworkManager::isPartitionRegistered(NesPartition nesPartition) const {
    return exchangeProtocol.getPartitionManager()->isRegistered(nesPartition);
}

uint64_t NetworkManager::registerSubpartitionConsumer(NesPartition nesPartition) {
    NES_DEBUG("NetworkManager: Registering SubpartitionConsumer: " << nesPartition.toString());
    return exchangeProtocol.getPartitionManager()->registerSubpartition(nesPartition);
}

uint64_t NetworkManager::unregisterSubpartitionConsumer(NesPartition nesPartition) {
    NES_INFO("NetworkManager: Unregistering SubpartitionConsumer: " << nesPartition.toString());
    return exchangeProtocol.getPartitionManager()->unregisterSubpartition(nesPartition);
}

OutputChannelPtr NetworkManager::registerSubpartitionProducer(const NodeLocation& nodeLocation, NesPartition nesPartition,
                                                            std::chrono::seconds waitTime, uint8_t retryTimes) {
    NES_INFO("NetworkManager: Registering SubpartitionProducer: " << nesPartition.toString());
    // method needs to return a pointer so that it can be passed to boost::thread_specific_ptr
    return OutputChannel::create(server->getContext(), nodeLocation.createZmqURI(), nesPartition, exchangeProtocol, waitTime, retryTimes);
}

}// namespace Network
}// namespace NES