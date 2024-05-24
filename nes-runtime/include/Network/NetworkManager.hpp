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

#ifndef NES_RUNTIME_INCLUDE_NETWORK_NETWORKMANAGER_HPP_
#define NES_RUNTIME_INCLUDE_NETWORK_NETWORKMANAGER_HPP_

#include <Network/ExchangeProtocol.hpp>
#include <Network/NetworkForwardRefs.hpp>
#include <Network/PartitionRegistrationStatus.hpp>
#include <Operators/LogicalOperators/Network/NodeLocation.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <chrono>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <string>

namespace NES::Network {

/**
 * @brief The NetworkManager manages creation and deletion of subpartition producer and consumer.
 */
class NetworkManager {
  public:
    /**
     * @brief create method to return a shared pointer of the NetworkManager
     * @param nodeEngineId,
     * @param hostname
     * @param port
     * @param exchangeProtocol
     * @param senderHighWatermark
     * @param numServerThread
     * @param connectSinksAsync if true, sinks will use a dedicated thread when attempting to establish a network channel
     * @param connectSourceEventChannelsAsync if true, source will use a dedicated thread when attempting to establish an event channel
     * @return the shared_ptr object
     */
    static NetworkManagerPtr create(WorkerId nodeEngineId,
                                    const std::string& hostname,
                                    uint16_t port,
                                    Network::ExchangeProtocol&& exchangeProtocol,
                                    const Runtime::BufferManagerPtr& bufferManager,
                                    int senderHighWatermark = -1,
                                    uint16_t numServerThread = DEFAULT_NUM_SERVER_THREADS,
                                    bool connectSinksAsync = false,
                                    bool connectSourceEventChannelsAsync = false);

    /**
     * @brief Creates a new network manager object, which comprises of a zmq server and an exchange protocol
     * @param nodeEngineId
     * @param hostname
     * @param port
     * @param exchangeProtocol
     * @param bufferManager
     * @param senderHighWatermark
     * @param numServerThread
     * @param connectSinksAsync if true, sinks will use a dedicated thread when attempting to establish a network channel
     * @param connectSourceEventChannelsAsync if true, source will use a dedicated thread when attempting to establish an event channel
     */
    explicit NetworkManager(WorkerId nodeEngineId,
                            const std::string& hostname,
                            uint16_t port,
                            ExchangeProtocol&& exchangeProtocol,
                            const Runtime::BufferManagerPtr& bufferManager,
                            int senderHighWatermark,
                            uint16_t numServerThread = DEFAULT_NUM_SERVER_THREADS,
                            bool connectSinksAsync = false,
                            bool connectSourceEventChannelsAsync = false);

    /**
     * @brief Destroy the network manager calling destroy()
     */
    ~NetworkManager();

    /**
     * @brief This method is called on the receiver side to register a SubpartitionConsumer, i.e. indicate that the
     * server is ready to receive particular subpartitions.
     * @param nesPartition the id of the logical channel between sender and receiver
     * @param senderLocation the network location of the sender
     * @param emitter underlying network source
     * @return true if the partition was registered for the first time, false otherwise
     */
    bool registerSubpartitionConsumer(const NesPartition& nesPartition,
                                      const NodeLocation& senderLocation,
                                      const DataEmitterPtr& emitter) const;

    /**
     * @brief This method is called on the receiver side to remove a SubpartitionConsumer.
     * @param nesPartition the id of the logical channel between sender and receiver
     * @return true if the partition was registered fully, false otherwise
     */
    bool unregisterSubpartitionConsumer(const NesPartition& nesPartition) const;

    /**
     * @brief This method is called on the receiver side to remove a SubpartitionConsumer.
     * @param nesPartition the id of the logical channel between sender and receiver
     * @return true if the partition was registered fully, false otherwise
     */
    bool unregisterSubpartitionProducer(const NesPartition& nesPartition) const;

    /**
     * Checks if a partition is registered
     * @param nesPartition the id of the logical channel between sender and receiver
     * @return true if the partition is registered
     */
    [[nodiscard]] PartitionRegistrationStatus isPartitionConsumerRegistered(const NesPartition& nesPartition) const;

    /**
     * Checks if a partition is registered
     * @param nesPartition the id of the logical channel between sender and receiver
     * @return true if the partition is registered
     */
    [[nodiscard]] PartitionRegistrationStatus isPartitionProducerRegistered(const NesPartition& nesPartition) const;

    /**
     * @brief This method is called on the sender side to register a SubpartitionProducer. If the connection to
     * the destination server is successful, a pointer to the DataChannel is returned, else nullptr is returned.
     * The DataChannel is not thread safe!
     * @param nodeLocation is the destination
     * @param nesPartition indicates the partition
     * @param waitTime time in seconds to wait until a retry is called
     * @param retryTimes times to retry a connection
     * @param version the version number which will be used by the receiver to determine if this channel will be accepted
     * @return the data network channel
     */
    NetworkChannelPtr registerSubpartitionProducer(const NodeLocation& nodeLocation,
                                                   const NesPartition& nesPartition,
                                                   Runtime::BufferManagerPtr bufferManager,
                                                   std::chrono::milliseconds waitTime,
                                                   uint8_t retryTimes,
                                                   DecomposedQueryPlanVersion version = 0);

    /**
     * @brief This method is called on the sender side to asynchronously register a SubpartitionProducer. It returns a future
     * that on completion will contain a pointer to the DataChannel if the connection to the destination server is successful, or
     * nullptr otherwise
     * The DataChannel is not thread safe!
     * @param nodeLocation is the destination
     * @param nesPartition indicates the partition
     * @param waitTime time in seconds to wait until a retry is called
     * @param retryTimes times to retry a connectio. Set this to zero to retry indefinitely.
     * @param reconfigurationMessage a message to be inserted into the query manager on completion, to inform the caller about
     * the completion of the operation
     * @param queryManager a pointer to the query manager which will hand over the reconfiguration message to the caller
     * @param version the version number which will be used by the receiver to determine if this channel will be accepted
     * @return a pair consisting of a future containing the data network channel on completion and a promise that aborts the connection process when
     * its value is set
     */
    std::pair<std::future<NetworkChannelPtr>, std::promise<bool>>
    registerSubpartitionProducerAsync(const NodeLocation& nodeLocation,
                                      const NesPartition& nesPartition,
                                      Runtime::BufferManagerPtr bufferManager,
                                      std::chrono::milliseconds waitTime,
                                      uint8_t retryTimes,
                                      Runtime::ReconfigurationMessage reconfigurationMessage,
                                      Runtime::QueryManagerPtr queryManager,
                                      DecomposedQueryPlanVersion version = 0);

    /**
     * @brief This method is called on the sender side to register a SubpartitionProducer. If the connection to
     * the destination server is successful, a pointer to the DataChannel is returned, else nullptr is returned.
     * The DataChannel is not thread safe!
     * @param nodeLocation is the destination
     * @param nesPartition indicates the partition
     * @param waitTime time in seconds to wait until a retry is called
     * @param retryTimes times to retry a connection
     * @return the event-only network channel
     */
    EventOnlyNetworkChannelPtr registerSubpartitionEventProducer(const NodeLocation& nodeLocation,
                                                                 const NesPartition& nesPartition,
                                                                 Runtime::BufferManagerPtr bufferManager,
                                                                 std::chrono::milliseconds waitTime,
                                                                 uint8_t retryTimes);

    /**
     * @brief
     * @param nodeLocation
     * @param nesPartition
     * @return
     */
    bool registerSubpartitionEventConsumer(const NodeLocation& nodeLocation,
                                           const NesPartition& nesPartition,
                                           Runtime::RuntimeEventListenerPtr eventListener);

    /**
     * @brief This methods destroys the network manager by stopping the underlying (ZMQ) server
     */
    void destroy();

    /**
     * @brief Returns the FQDN of the network manager
     * @return the network location of the network manager
     */
    NodeLocation getServerLocation() const;

    /**
     * @brief Returns the server data port
     * @return the server data port
     */
    uint16_t getServerDataPort() const;

    /**
     * @brief This method is called to asynchronously register an event produce. It returns a future
     * that on completion will contain a pointer to the EventChannel if the connection to the destination server is successful, or
     * nullptr otherwise
     * @param nodeLocation is the destination
     * @param nesPartition indicates the partition
     * @param bufferManager a pointer the buffer manager
     * @param waitTime time in seconds to wait until a retry is called
     * @param retryTimes times to retry a connectio.
     * @param version the version number which will be used by the receiver to determine if this channel will be accepted
     * @return a pair consisting of a future containing the data network channel on completion and a promise that aborts the connection process when
     * its value is set (abortion not yet implemented)
     */
    //todo #4490: implement aborting connection attempt if returned promise is set
    std::pair<std::future<EventOnlyNetworkChannelPtr>, std::promise<bool>>
    registerSubpartitionEventProducerAsync(const NodeLocation& nodeLocation,
                                           const NesPartition& nesPartition,
                                           Runtime::BufferManagerPtr bufferManager,
                                           std::chrono::milliseconds waitTime,
                                           uint8_t retryTimes);

    /**
     * @brief retrieve the value of the connectSinkAsync flag which indicates if a separate thread should be used to establish
     * network channels
     * @return the value of the connectSinkAsync flag
     */
    bool getConnectSinksAsync();

    /**
     * @brief retrieve the value of the connectSourceEventChannelsAsync flag which indicates if a separate thread should be used to establish
     * event channels from sources to sinks
     * @return the value of the connectSinkAsync flag
     */
    bool getConnectSourceEventChannelsAsync();

  private:
    NodeLocation nodeLocation;
    ZmqServerPtr server;
    ExchangeProtocol exchangeProtocol;
    PartitionManagerPtr partitionManager{nullptr};
    int senderHighWatermark;
    const bool connectSinksAsync;
    const bool connectSourceEventChannelsAsync;
};

}// namespace NES::Network

#endif// NES_RUNTIME_INCLUDE_NETWORK_NETWORKMANAGER_HPP_
