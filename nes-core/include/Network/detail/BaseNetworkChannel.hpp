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

#ifndef NES_INCLUDE_NETWORK_DETAIL_BASENETWORKCHANNEL_HPP_
#define NES_INCLUDE_NETWORK_DETAIL_BASENETWORKCHANNEL_HPP_

#include <Network/ChannelId.hpp>
#include <Network/NetworkMessage.hpp>
#include <memory>
#include <zmq.hpp>

namespace NES::Runtime {
class BufferManager;
using BufferManagerPtr = std::shared_ptr<BufferManager>;
}// namespace NES::Runtime

namespace NES::Network::detail {

/**
 * @brief This is the base class for a network channel with support to connection init and close.
 */
class BaseNetworkChannel {
  public:
    static constexpr bool canSendData = false;
    static constexpr bool canSendEvent = false;

    /**
     * @brief Creates a new BaseNetworkChannel
     * @param zmqSocket zmq socket connection
     * @param channelId the id of the channel
     * @param address remote address
     * @param bufferManager the buffer manager
     */
    explicit BaseNetworkChannel(zmq::socket_t&& zmqSocket,
                                ChannelId channelId,
                                std::string&& address,
                                Runtime::BufferManagerPtr&& bufferManager);

    /**
     * @brief Method to handle the error
     * @param the error message
     */
    void onError(Messages::ErrorMessage& errorMsg);

    /**
     * Close the channel and send EndOfStream message to consumer
     * @param isEventOnly whether the channel is for events only
     */
    void close(bool isEventOnly);

  protected:
    const std::string socketAddr;
    zmq::socket_t zmqSocket;
    const ChannelId channelId;
    bool isClosed{false};
    Runtime::BufferManagerPtr bufferManager;
};

}// namespace NES::Network::detail
#endif  // NES_INCLUDE_NETWORK_DETAIL_BASENETWORKCHANNEL_HPP_
