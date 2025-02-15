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
#include <Network/NetworkSource.hpp>
#include <Network/PartitionManager.hpp>
#include <Network/ZmqServer.hpp>
#include <Runtime/QueryManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES::Network {

NetworkManager::NetworkManager(WorkerId nodeEngineId,
                               const std::string& hostname,
                               uint16_t port,
                               ExchangeProtocol&& exchangeProtocol,
                               const Runtime::BufferManagerPtr& bufferManager,
                               int senderHighWatermark,
                               uint16_t numServerThread,
                               bool connectSinksAsync,
                               bool connectSourceEventChannelsAsync)
    : nodeLocation(), server(std::make_unique<ZmqServer>(hostname, port, numServerThread, this->exchangeProtocol, bufferManager)),
      exchangeProtocol(std::move(exchangeProtocol)), partitionManager(this->exchangeProtocol.getPartitionManager()),
      senderHighWatermark(senderHighWatermark), connectSinksAsync(connectSinksAsync),
      connectSourceEventChannelsAsync(connectSourceEventChannelsAsync) {

    if (bool const success = server->start(); success) {
        nodeLocation = NodeLocation(nodeEngineId, hostname, server->getServerPort());
        NES_INFO("NetworkManager: Server started successfully on {}", nodeLocation.createZmqURI());
    } else {
        NES_THROW_RUNTIME_ERROR("NetworkManager: Server failed to start on " << hostname << ":" << port);
    }
}

NetworkManager::~NetworkManager() { destroy(); }

NetworkManagerPtr NetworkManager::create(WorkerId nodeEngineId,
                                         const std::string& hostname,
                                         uint16_t port,
                                         Network::ExchangeProtocol&& exchangeProtocol,
                                         const Runtime::BufferManagerPtr& bufferManager,
                                         int senderHighWatermark,
                                         uint16_t numServerThread,
                                         bool connectSinksAsync,
                                         bool connectSourceEventChannelsAsync) {
    return std::make_shared<NetworkManager>(nodeEngineId,
                                            hostname,
                                            port,
                                            std::move(exchangeProtocol),
                                            bufferManager,
                                            senderHighWatermark,
                                            numServerThread,
                                            connectSinksAsync,
                                            connectSourceEventChannelsAsync);
}

void NetworkManager::destroy() { server->stop(); }

PartitionRegistrationStatus NetworkManager::isPartitionConsumerRegistered(const NesPartition& nesPartition) const {
    return partitionManager->getConsumerRegistrationStatus(nesPartition);
}

PartitionRegistrationStatus NetworkManager::isPartitionProducerRegistered(const NesPartition& nesPartition) const {
    return partitionManager->getProducerRegistrationStatus(nesPartition);
}

NodeLocation NetworkManager::getServerLocation() const { return nodeLocation; }

uint16_t NetworkManager::getServerDataPort() const { return server->getServerPort(); }

bool NetworkManager::registerSubpartitionConsumer(const NesPartition& nesPartition,
                                                  const NodeLocation& senderLocation,
                                                  const DataEmitterPtr& emitter) const {
    NES_DEBUG("NetworkManager: Registering SubpartitionConsumer: {} from {}",
              nesPartition.toString(),
              senderLocation.getHostname());
    NES_ASSERT2_FMT(emitter, "invalid network source " << nesPartition.toString());
    return partitionManager->registerSubpartitionConsumer(nesPartition, senderLocation, emitter);
}

bool NetworkManager::unregisterSubpartitionConsumer(const NesPartition& nesPartition) const {
    NES_DEBUG("NetworkManager: Unregistering SubpartitionConsumer: {}", nesPartition.toString());
    return partitionManager->unregisterSubpartitionConsumer(nesPartition);
}

bool NetworkManager::unregisterSubpartitionProducer(const NesPartition& nesPartition) const {
    NES_DEBUG("NetworkManager: Unregistering SubpartitionProducer: {}", nesPartition.toString());
    return partitionManager->unregisterSubpartitionProducer(nesPartition);
}

NetworkChannelPtr NetworkManager::registerSubpartitionProducer(const NodeLocation& nodeLocation,
                                                               const NesPartition& nesPartition,
                                                               Runtime::BufferManagerPtr bufferManager,
                                                               std::chrono::milliseconds waitTime,
                                                               uint8_t retryTimes,
                                                               DecomposedQueryPlanVersion version) {
    NES_DEBUG("NetworkManager: Registering SubpartitionProducer: {}", nesPartition.toString());
    partitionManager->registerSubpartitionProducer(nesPartition, nodeLocation);
    return NetworkChannel::create(server->getContext(),
                                  nodeLocation.createZmqURI(),
                                  nesPartition,
                                  exchangeProtocol,
                                  std::move(bufferManager),
                                  senderHighWatermark,
                                  waitTime,
                                  retryTimes,
                                  version);
}

std::pair<std::future<NetworkChannelPtr>, std::promise<bool>>
NetworkManager::registerSubpartitionProducerAsync(const NodeLocation& nodeLocation,
                                                  const NesPartition& nesPartition,
                                                  Runtime::BufferManagerPtr bufferManager,
                                                  std::chrono::milliseconds waitTime,
                                                  uint8_t retryTimes,
                                                  Runtime::ReconfigurationMessage reconfigurationMessage,
                                                  Runtime::QueryManagerPtr queryManager,
                                                  DecomposedQueryPlanVersion version) {
    NES_DEBUG("NetworkManager: Asynchronously registering SubpartitionProducer: {}", nesPartition.toString());
    partitionManager->registerSubpartitionProducer(nesPartition, nodeLocation);

    //create a promise that will be used to hand the channel back to the network sink that triggered its creation
    std::promise<NetworkChannelPtr> promise;
    auto future = promise.get_future();

    //create a promise that is passed back to the caller and can be used to abort the connection process
    std::promise<bool> abortConnectionPromise;
    auto abortConnectionFuture = abortConnectionPromise.get_future();

    //start thread
    //todo #4309: instead of starting one thread per connection attempt, hand this work to a designated thread pool
    std::thread thread([zmqContext = server->getContext(),
                        nodeLocation,
                        nesPartition,
                        protocol = exchangeProtocol,
                        bufferManager = std::move(bufferManager),
                        highWaterMark = senderHighWatermark,
                        waitTime,
                        retryTimes,
                        promise = std::move(promise),
                        queryManager,
                        reconfigurationMessage,
                        version,
                        abortConnectionFuture = std::move(abortConnectionFuture)]() mutable {
        //wrap the abort-connection-future in and optional because the create function expects an optional as a parameter
        auto future_optional = std::make_optional<std::future<bool>>(std::move(abortConnectionFuture));

        //create the channel
        auto channel = NetworkChannel::create(zmqContext,
                                              nodeLocation.createZmqURI(),
                                              nesPartition,
                                              protocol,
                                              std::move(bufferManager),
                                              highWaterMark,
                                              waitTime,
                                              retryTimes,
                                              version,
                                              std::move(future_optional));

        NES_ERROR("channel creation done, setting promise")
        //pass channel back to calling thread via promise
        promise.set_value(std::move(channel));

        NES_ERROR("promise set, adding reconf message")

        //notify the sink about successful connection via reconfiguration message
        queryManager->addReconfigurationMessage(reconfigurationMessage.getQueryId(),
                                                reconfigurationMessage.getParentPlanId(),
                                                reconfigurationMessage.getParentPlanVersion(),
                                                reconfigurationMessage,
                                                false);
    });

    thread.detach();
    return {std::move(future), std::move(abortConnectionPromise)};
}

EventOnlyNetworkChannelPtr NetworkManager::registerSubpartitionEventProducer(const NodeLocation& nodeLocation,
                                                                             const NesPartition& nesPartition,
                                                                             Runtime::BufferManagerPtr bufferManager,
                                                                             std::chrono::milliseconds waitTime,
                                                                             uint8_t retryTimes) {
    NES_DEBUG("NetworkManager: Registering SubpartitionEvent Producer: {}", nesPartition.toString());
    return EventOnlyNetworkChannel::create(server->getContext(),
                                           nodeLocation.createZmqURI(),
                                           nesPartition,
                                           exchangeProtocol,
                                           std::move(bufferManager),
                                           senderHighWatermark,
                                           waitTime,
                                           retryTimes);
}
std::pair<std::future<EventOnlyNetworkChannelPtr>, std::promise<bool>>
NetworkManager::registerSubpartitionEventProducerAsync(const NodeLocation& nodeLocation,
                                                       const NesPartition& nesPartition,
                                                       Runtime::BufferManagerPtr bufferManager,
                                                       std::chrono::milliseconds waitTime,
                                                       uint8_t retryTimes) {
    NES_DEBUG("NetworkManager: Registering SubpartitionEvent Producer: {}", nesPartition.toString());
    //create a promise that will be used to hand the channel back to the network sink that triggered its creation
    std::promise<EventOnlyNetworkChannelPtr> promise;
    auto future = promise.get_future();

    //create a promise that is passed back to the caller and can be used to abort the connection process
    std::promise<bool> abortConnectionPromise;
    auto abortConnectionFuture = abortConnectionPromise.get_future();

    //start thread
    //todo #4309: instead of starting one thread per connection attempt, hand this work to a designated thread pool
    std::thread thread([zmqContext = server->getContext(),
                        nodeLocation,
                        nesPartition,
                        protocol = exchangeProtocol,
                        bufferManager = std::move(bufferManager),
                        highWaterMark = senderHighWatermark,
                        waitTime,
                        retryTimes,
                        promise = std::move(promise),
                        abortConnectionFuture = std::move(abortConnectionFuture)]() mutable {
        //wrap the abort-connection-future in and optional because the create function expects an optional as a parameter
        auto future_optional = std::make_optional<std::future<bool>>(std::move(abortConnectionFuture));
        (void) std::move(future_optional);
        auto channel = EventOnlyNetworkChannel::create(zmqContext,
                                                       nodeLocation.createZmqURI(),
                                                       nesPartition,
                                                       protocol,
                                                       std::move(bufferManager),
                                                       highWaterMark,
                                                       waitTime,
                                                       retryTimes);//todo: insert stop future

        //pass channel back to calling thread via promise
        promise.set_value(std::move(channel));
    });

    thread.detach();
    return {std::move(future), std::move(abortConnectionPromise)};
}

bool NetworkManager::registerSubpartitionEventConsumer(const NodeLocation& nodeLocation,
                                                       const NesPartition& nesPartition,
                                                       Runtime::RuntimeEventListenerPtr eventListener) {
    // note that this increases the subpartition producer counter by one
    // we want to do so to keep the partition alive until all outbound network channel + the inbound event channel are in-use
    NES_DEBUG("NetworkManager: Registering Subpartition Event Consumer: {}", nesPartition.toString());
    return partitionManager->addSubpartitionEventListener(nesPartition, nodeLocation, eventListener);
}

bool NetworkManager::getConnectSinksAsync() { return connectSinksAsync; }

bool NetworkManager::getConnectSourceEventChannelsAsync() { return connectSourceEventChannelsAsync; }

DataSourcePtr NetworkManager::getNetworkSourceWithPartition(NesPartition partition) {
    auto dataEmitter = partitionManager->getDataEmitter(partition);
    return std::dynamic_pointer_cast<DataSource>(dataEmitter);
}
}// namespace NES::Network
