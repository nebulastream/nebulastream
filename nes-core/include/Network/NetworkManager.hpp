/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_NETWORK_NETWORKMANAGER_HPP_
#define NES_INCLUDE_NETWORK_NETWORKMANAGER_HPP_

#include <Network/ExchangeProtocol.hpp>
#include <Network/NetworkForwardRefs.hpp>
#include <Network/PartitionRegistrationStatus.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <cstdint>
#include <functional>
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
     * @param numServerThread
     * @return the shared_ptr object
     */
    static NetworkManagerPtr create(uint64_t nodeEngineId,
                                    const std::string& hostname,
                                    uint16_t port,
                                    Network::ExchangeProtocol&& exchangeProtocol,
                                    const Runtime::BufferManagerPtr& bufferManager,
                                    uint16_t numServerThread = DEFAULT_NUM_SERVER_THREADS);

    /**
     * @brief Creates a new network manager object, which comprises of a zmq server and an exchange protocol
     * @param nodeEngineId
     * @param hostname
     * @param port
     * @param exchangeProtocol
     * @param bufferManager
     * @param numServerThread
     */
    explicit NetworkManager(uint64_t nodeEngineId,
                            const std::string& hostname,
                            uint16_t port,
                            ExchangeProtocol&& exchangeProtocol,
                            const Runtime::BufferManagerPtr& bufferManager,
                            uint16_t numServerThread = DEFAULT_NUM_SERVER_THREADS);

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
     * @return the data network channel
     */
    NetworkChannelPtr registerSubpartitionProducer(const NodeLocation& nodeLocation,
                                                   const NesPartition& nesPartition,
                                                   Runtime::BufferManagerPtr bufferManager,
                                                   std::chrono::milliseconds waitTime,
                                                   uint8_t retryTimes);

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

  private:
    NodeLocation nodeLocation;
    ZmqServerPtr server;
    ExchangeProtocol exchangeProtocol;
    PartitionManagerPtr partitionManager{nullptr};
};

}// namespace NES::Network

#endif  // NES_INCLUDE_NETWORK_NETWORKMANAGER_HPP_
