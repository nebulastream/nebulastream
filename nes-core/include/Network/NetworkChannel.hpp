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

#ifndef NES_INCLUDE_NETWORK_OUTPUT_CHANNEL_HPP_
#define NES_INCLUDE_NETWORK_OUTPUT_CHANNEL_HPP_

#include <Network/NetworkForwardRefs.hpp>
#include <Network/detail/BaseNetworkChannel.hpp>
#include <Network/detail/NetworkDataSender.hpp>
#include <Network/detail/NetworkEventSender.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>

namespace NES {
namespace Network {

class ExchangeProtocol;

/**
 * @brief This class represent the network channel for data transfer that NES uses to send data among nodes.
 * This class can send data and event packets.
 * This class is not thread-safe.
 */
class NetworkChannel : public detail::NetworkEventSender<detail::NetworkDataSender<detail::BaseNetworkChannel>> {
    using inherited = detail::NetworkEventSender<detail::NetworkDataSender<detail::BaseNetworkChannel>>;

  public:
    static constexpr bool canSendData = inherited::canSendData;
    static constexpr bool canSendEvent = inherited::canSendEvent;

    /**
     * @brief Creates a network channel instance with the given parameters
     * @param zmqContext the local zmq server context
     * @param channelId the remote nes channel id to connect to
     * @param address the socket address of the remote server
     * @param bufferManager the buffer manager
     */
    explicit NetworkChannel(zmq::socket_t&& zmqSocket,
                            ChannelId channelId,
                            std::string&& address,
                            Runtime::BufferManagerPtr bufferManager);

    /**
     * @brief close the output channel and release resources
     */
    ~NetworkChannel();

    NetworkChannel(const NetworkChannel&) = delete;

    NetworkChannel& operator=(const NetworkChannel&) = delete;

    /**
     * @brief Closes the underlying network connection
     */
    void close();

    /**
     * @brief Creates a network channel instance with the given parameters
     * @param zmqContext the local zmq server context
     * @param address the ip address of the remote server
     * @param nesPartition the remote nes partition to connect to
     * @param protocol the protocol implementation
     * @param bufferManager the buffer manager
     * @param waitTime the backoff time in case of failure when connecting
     * @param retryTimes the number of retries before the methods will raise error
     * @return the network channel or nullptr on error
     */
    static NetworkChannelPtr create(const std::shared_ptr<zmq::context_t>& zmqContext,
                                    std::string&& socketAddr,
                                    NesPartition nesPartition,
                                    ExchangeProtocol& protocol,
                                    Runtime::BufferManagerPtr bufferManager,
                                    std::chrono::milliseconds waitTime,
                                    uint8_t retryTimes);
};

/**
 * @brief This class represent the network channel for event transfer that NES uses to send events among nodes.
 * This class can send only event packets.
 * This class is not thread-safe.
 */
class EventOnlyNetworkChannel : public detail::NetworkEventSender<detail::BaseNetworkChannel> {
    using inherited = detail::NetworkEventSender<detail::BaseNetworkChannel>;

  public:
    /**
     * @brief Creates a network channel for events-only instance with the given parameters
     * @param zmqContext the local zmq server context
     * @param channelId the remote nes channel id to connect to
     * @param address the socket address of the remote server
     * @param bufferManager the buffer manager
     */
    explicit EventOnlyNetworkChannel(zmq::socket_t&& zmqSocket,
                                     ChannelId channelId,
                                     std::string&& address,
                                     Runtime::BufferManagerPtr bufferManager);

    /**
     * @brief close the output channel and release resources
     */
    ~EventOnlyNetworkChannel();

    EventOnlyNetworkChannel(const NetworkChannel&) = delete;

    EventOnlyNetworkChannel& operator=(const NetworkChannel&) = delete;

    /**
     * @brief Closes the underlying network connection
     */
    void close();

    /**
     * @brief Creates a networkf channel instance for event transmission with the given parameters
     * @param zmqContext the local zmq server context
     * @param address the ip address of the remote server
     * @param nesPartition the remote nes partition to connect to
     * @param protocol the protocol implementation
     * @param waitTime the backoff time in case of failure when connecting
     * @param retryTimes the number of retries before the methods will raise error
     * @return the network channel or nullptr on error
     */
    static EventOnlyNetworkChannelPtr create(const std::shared_ptr<zmq::context_t>& zmqContext,
                                             std::string&& socketAddr,
                                             NesPartition nesPartition,
                                             ExchangeProtocol& protocol,
                                             Runtime::BufferManagerPtr bufferManager,
                                             std::chrono::milliseconds waitTime,
                                             uint8_t retryTimes);
};

}// namespace Network
}// namespace NES

#endif// NES_INCLUDE_NETWORK_OUTPUT_CHANNEL_HPP_
