/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <Network/NetworkChannel.hpp>
#include <Network/NetworkManager.hpp>
#include <Network/PartitionManager.hpp>
#include <Network/ZmqServer.hpp>
#include <Util/Logger.hpp>

namespace NES::Network {

NetworkManager::NetworkManager(uint64_t nodeEngineId,
                               const std::string& hostname,
                               uint16_t port,
                               ExchangeProtocol&& exchangeProtocol,
                               const Runtime::BufferManagerPtr& bufferManager,
                               uint16_t numServerThread)
    : nodeLocation(),
      server(std::make_unique<ZmqServer>(hostname, port, numServerThread, this->exchangeProtocol, bufferManager)),
      exchangeProtocol(std::move(exchangeProtocol)), partitionManager(this->exchangeProtocol.getPartitionManager()) {

    if (bool const success = server->start(); success) {
        nodeLocation = NodeLocation(nodeEngineId, hostname, server->getServerPort());
        NES_INFO("NetworkManager: Server started successfully");
    } else {
        NES_THROW_RUNTIME_ERROR("NetworkManager: Server failed to start on " << hostname << ":" << port);
    }
}

NetworkManager::~NetworkManager() { destroy(); }

NetworkManagerPtr NetworkManager::create(uint64_t nodeEngineId,
                                         const std::string& hostname,
                                         uint16_t port,
                                         Network::ExchangeProtocol&& exchangeProtocol,
                                         const Runtime::BufferManagerPtr& bufferManager,
                                         uint16_t numServerThread) {
    return std::make_shared<NetworkManager>(nodeEngineId,
                                            hostname,
                                            port,
                                            std::move(exchangeProtocol),
                                            bufferManager,
                                            numServerThread);
}

void NetworkManager::destroy() { server->stop(); }

PartitionRegistrationStatus NetworkManager::isPartitionConsumerRegistered(const NesPartition& nesPartition) const {
    return partitionManager->getConsumerRegistrationStatus(nesPartition);
}

PartitionRegistrationStatus NetworkManager::isPartitionProducerRegistered(const NesPartition& nesPartition) const {
    return partitionManager->getProducerRegistrationStatus(nesPartition);
}

NodeLocation NetworkManager::getServerLocation() const {
    return nodeLocation;
}

uint16_t NetworkManager::getServerDataPort() const {
    return server->getServerPort();
}

bool NetworkManager::registerSubpartitionConsumer(const NesPartition& nesPartition,
                                                  const NodeLocation& senderLocation,
                                                  const DataEmitterPtr& emitter) const {
    NES_DEBUG("NetworkManager: Registering SubpartitionConsumer: " << nesPartition.toString() << " from "
                                                                   << senderLocation.getHostname());
    NES_ASSERT2_FMT(emitter, "invalid network source " << nesPartition.toString());
    return partitionManager->registerSubpartitionConsumer(nesPartition, senderLocation, emitter);
}

bool NetworkManager::unregisterSubpartitionConsumer(const NesPartition& nesPartition) const {
    NES_INFO("NetworkManager: Unregistering SubpartitionConsumer: " << nesPartition.toString());
    return partitionManager->unregisterSubpartitionConsumer(nesPartition);
}

bool NetworkManager::unregisterSubpartitionProducer(const NesPartition& nesPartition) const {
    NES_INFO("NetworkManager: Unregistering SubpartitionProducer: " << nesPartition.toString());
    return partitionManager->unregisterSubpartitionProducer(nesPartition);
}

NetworkChannelPtr NetworkManager::registerSubpartitionProducer(const NodeLocation& nodeLocation,
                                                               const NesPartition& nesPartition,
                                                               Runtime::BufferManagerPtr bufferManager,
                                                               std::chrono::milliseconds waitTime,
                                                               uint8_t retryTimes) {
    NES_INFO("NetworkManager: Registering SubpartitionProducer: " << nesPartition.toString());
    partitionManager->registerSubpartitionProducer(nesPartition, nodeLocation);
    return NetworkChannel::create(server->getContext(),
                                  nodeLocation.createZmqURI(),
                                  nesPartition,
                                  exchangeProtocol,
                                  std::move(bufferManager),
                                  waitTime,
                                  retryTimes);
}

EventOnlyNetworkChannelPtr NetworkManager::registerSubpartitionEventProducer(const NodeLocation& nodeLocation,
                                                                             const NesPartition& nesPartition,
                                                                             Runtime::BufferManagerPtr bufferManager,
                                                                             std::chrono::milliseconds waitTime,
                                                                             uint8_t retryTimes) {
    NES_INFO("NetworkManager: Registering SubpartitionEvent Producer: " << nesPartition.toString());
    return EventOnlyNetworkChannel::create(server->getContext(),
                                           nodeLocation.createZmqURI(),
                                           nesPartition,
                                           exchangeProtocol,
                                           std::move(bufferManager),
                                           waitTime,
                                           retryTimes);
}

bool NetworkManager::registerSubpartitionEventConsumer(const NodeLocation& nodeLocation,
                                                       const NesPartition& nesPartition,
                                                       Runtime::RuntimeEventListenerPtr eventListener) {
    // note that this increases the subpartition producer counter by one
    // we want to do so to keep the partition alive until all outbound network channel + the inbound event channel are in-use
    NES_INFO("NetworkManager: Registering SubpartitionEvent Consumer: " << nesPartition.toString());
    return partitionManager->addSubpartitionEventListener(nesPartition, nodeLocation, eventListener);
}

}// namespace NES::Network