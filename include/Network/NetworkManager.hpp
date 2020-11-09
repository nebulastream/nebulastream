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

#ifndef NES_NETWORKDISPATCHER_HPP
#define NES_NETWORKDISPATCHER_HPP

#include <Network/ExchangeProtocol.hpp>
#include <Network/NesPartition.hpp>
#include <Network/NodeLocation.hpp>
#include <Network/OutputChannel.hpp>
#include <Network/PartitionManager.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace NES {

namespace Network {

class ZmqServer;
class OutputChannel;

/**
 * @brief The NetworkManager manages creation and deletion of subpartition producer and consumer.
 */
class NetworkManager {
  public:
    /**
     * @brief create method to return a shared pointer of the NetworkManager
     * @param hostname
     * @param port
     * @param exchangeProtocol
     * @param numServerThread
     * @return the shared_ptr object
     */
    static std::shared_ptr<NetworkManager> create(
        const std::string& hostname,
        uint16_t port,
        Network::ExchangeProtocol&& exchangeProtocol,
        BufferManagerPtr bufferManager,
        uint16_t numServerThread = DEFAULT_NUM_SERVER_THREADS);

    /**
     * @brief This method is called on the receiver side to register a SubpartitionConsumer, i.e. indicate that the
     * server is ready to receive particular subpartitions.
     * @param the nesPartition
     * @return the current counter of the subpartition
     */
    uint64_t registerSubpartitionConsumer(NesPartition nesPartition);

    /**
     * @brief This method is called on the receiver side to remove a SubpartitionConsumer.
     * @param the nesPartition
     * @return the new counter of the subpartition
     */
    uint64_t unregisterSubpartitionConsumer(NesPartition nesPartition);

    /**
     * @param nesPartition to check
     * @return true if the partition is registered
     */
    bool isPartitionRegistered(NesPartition nesPartition) const;

    /**
     * @brief This method is called on the sender side to register a SubpartitionProducer. If the connection to
     * the destination server is successful, a pointer to the OutputChannel is returned, else nullptr is returned.
     * The OutputChannel is not thread safe!
     * @param nodeLocation is the destination
     * @param nesPartition indicates the partition
     * @param waitTime time in seconds to wait until a retry is called
     * @param retryTimes times to retry a connection
     * @return
     */
    OutputChannelPtr registerSubpartitionProducer(const NodeLocation& nodeLocation, NesPartition nesPartition,
                                                  std::chrono::seconds waitTime, uint8_t retryTimes);

    explicit NetworkManager(const std::string& hostname, uint16_t port, ExchangeProtocol&& exchangeProtocol, BufferManagerPtr bufferManager,
                            uint16_t numServerThread = DEFAULT_NUM_SERVER_THREADS);

    std::shared_ptr<ZmqServer> server;
    ExchangeProtocol exchangeProtocol;
};
typedef std::shared_ptr<NetworkManager> NetworkManagerPtr;

}// namespace Network
}// namespace NES

#endif//NES_NETWORKDISPATCHER_HPP
