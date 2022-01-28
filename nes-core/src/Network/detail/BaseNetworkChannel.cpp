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

#include <Network/NetworkMessage.hpp>
#include <Network/ZmqUtils.hpp>
#include <Network/detail/BaseNetworkChannel.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Logger.hpp>

namespace NES::Network::detail {
BaseNetworkChannel::BaseNetworkChannel(zmq::socket_t&& zmqSocket,
                                       const ChannelId channelId,
                                       std::string&& address,
                                       Runtime::BufferManagerPtr&& bufferManager)
    : socketAddr(std::move(address)), zmqSocket(std::move(zmqSocket)), channelId(channelId),
      bufferManager(std::move(bufferManager)) {}

void BaseNetworkChannel::onError(Messages::ErrorMessage& errorMsg) { NES_ERROR(errorMsg.getErrorTypeAsString()); }

void BaseNetworkChannel::close(bool isEventOnly) {
    if (isClosed) {
        return;
    }
    if (isEventOnly) {
        sendMessage<Messages::EndOfStreamMessage>(zmqSocket, channelId, Messages::ChannelType::EventOnlyChannel);
    } else {
        sendMessage<Messages::EndOfStreamMessage>(zmqSocket, channelId, Messages::ChannelType::DataChannel);
    }
    zmqSocket.close();
    NES_DEBUG("Socket(" << socketAddr << ") closed for " << channelId << (isEventOnly ? " Event" : " Data"));
    isClosed = true;
}
}// namespace NES::Network::detail