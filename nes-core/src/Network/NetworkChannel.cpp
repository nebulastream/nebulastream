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
#include <Network/ExchangeProtocol.hpp>
#include <Network/NetworkChannel.hpp>
#include <Network/NetworkMessage.hpp>
#include <Runtime/BufferManager.hpp>

namespace NES::Network {
/// 60s as linger time: http://api.zeromq.org/2-1:zmq-setsockopt
static constexpr int DEFAULT_LINGER_VALUE = 10 * 1000;
/// 10s as ZMQ_RCVTIMEO: Maximum time before a recv operation returns with EAGAIN : http://api.zeromq.org/3-0:zmq-setsockopt
static constexpr int DEFAULT_RCVTIMEO_VALUE = 10 * 1000;
namespace detail {
template<typename T>
std::unique_ptr<T> createNetworkChannel(std::shared_ptr<zmq::context_t> const& zmqContext,
                                        std::string&& socketAddr,
                                        NesPartition nesPartition,
                                        ExchangeProtocol& protocol,
                                        Runtime::BufferManagerPtr bufferManager,
                                        std::chrono::milliseconds waitTime,
                                        uint8_t retryTimes) {
    // TODO create issue to make the network channel allocation async
    // TODO currently, we stall the worker threads
    // TODO as a result, this kills performance of running queries if we submit a new query
    std::chrono::milliseconds backOffTime = waitTime;
    constexpr auto nameHelper = []() {
        if constexpr (std::is_same_v<T, EventOnlyNetworkChannel>) {
            return "EventOnlyNetworkChannel";
        } else {
            return "DataChannel";
        }
    };
    constexpr auto channelName = nameHelper();
    try {
        const int linger = DEFAULT_LINGER_VALUE;
        const int rcvtimeo = DEFAULT_RCVTIMEO_VALUE;
        ChannelId channelId(nesPartition, Runtime::NesThread::getId());
        zmq::socket_t zmqSocket(*zmqContext, ZMQ_DEALER);
        NES_DEBUG(channelName << ": Connecting with zmq-socketopt linger=" << std::to_string(linger) << ", id=" << channelId);
        zmqSocket.set(zmq::sockopt::linger, linger);
        // Sets the timeout for receive operation on the socket. If the value is 0, zmq_recv(3) will return immediately,
        // with a EAGAIN error if there is no message to receive. If the value is -1, it will block until a message is available.
        // For all other values, it will wait for a message for that amount of time before returning with an EAGAIN error.
        zmqSocket.set(zmq::sockopt::rcvtimeo, rcvtimeo);

        zmqSocket.set(zmq::sockopt::routing_id, zmq::const_buffer{&channelId, sizeof(ChannelId)});
        zmqSocket.connect(socketAddr);
        int i = 0;
        constexpr auto mode = (T::canSendEvent && !T::canSendData) ? Network::Messages::ChannelType::EventOnlyChannel
                                                                   : Network::Messages::ChannelType::DataChannel;
        while (i < retryTimes) {

            sendMessage<Messages::ClientAnnounceMessage>(zmqSocket, channelId, mode);

            zmq::message_t recvHeaderMsg;
            auto optRecvStatus = zmqSocket.recv(recvHeaderMsg, kZmqRecvDefault);
            if constexpr (mode == Network::Messages::ChannelType::EventOnlyChannel) {
                if (!optRecvStatus.has_value()) {
                    NES_DEBUG("recv failed on network channel");
                    return nullptr;
                }
            }
            NES_ASSERT2_FMT(optRecvStatus.has_value(), "invalid recv");

            auto* recvHeader = recvHeaderMsg.data<Messages::MessageHeader>();

            if (recvHeader->getMagicNumber() != Messages::NES_NETWORK_MAGIC_NUMBER) {
                NES_THROW_RUNTIME_ERROR("OutputChannel: Message from server is corrupt!");
            }

            switch (recvHeader->getMsgType()) {
                case Messages::MessageType::ServerReady: {
                    zmq::message_t recvMsg;
                    auto optRecvStatus2 = zmqSocket.recv(recvMsg, kZmqRecvDefault);
                    NES_ASSERT2_FMT(optRecvStatus2.has_value(), "invalid recv");
                    auto* serverReadyMsg = recvMsg.data<Messages::ServerReadyMessage>();
                    // check if server responds with a ServerReadyMessage
                    // check if the server has the correct corresponding channel registered, this is guaranteed by matching IDs
                    if (!(serverReadyMsg->getChannelId().getNesPartition() == channelId.getNesPartition())) {
                        NES_ERROR(channelName << ": Connection failed with server " << socketAddr << " for "
                                              << channelId.getNesPartition().toString()
                                              << "->Wrong server ready message! Reason: Partitions are not matching");
                        break;
                    }
                    NES_DEBUG(channelName << ": Connection established with server " << socketAddr << " for " << channelId);
                    return std::make_unique<T>(std::move(zmqSocket), channelId, std::move(socketAddr), std::move(bufferManager));
                }
                case Messages::MessageType::ErrorMessage: {
                    // if server receives a message that an error occurred
                    zmq::message_t errorEnvelope;
                    auto optRecvStatus3 = zmqSocket.recv(errorEnvelope, kZmqRecvDefault);
                    NES_ASSERT2_FMT(optRecvStatus3.has_value(), "invalid recv");
                    auto errorMsg = *errorEnvelope.data<Messages::ErrorMessage>();
                    if (errorMsg.isPartitionDeleted()) {
                        if constexpr (std::is_same_v<T, EventOnlyNetworkChannel>) {
                            // for an event-only channel it's ok to get this message
                            // it means the producer is already done so it wont be able
                            // to receive any event. We should figure out if this case must be
                            // handled somewhere else. For instance, what does this mean for FT and upstream backup?
                            NES_ERROR("EventOnlyNetworkChannel: Received partition deleted error from server " << socketAddr);
                            return nullptr;
                        }
                    }

                    NES_ERROR(channelName << ": Received error from server-> " << errorMsg.getErrorTypeAsString());
                    protocol.onChannelError(errorMsg);
                    break;
                }
                default: {
                    // got a wrong message type!
                    NES_ERROR(channelName << ": received unknown message " << static_cast<int>(recvHeader->getMsgType()));
                    return nullptr;
                }
            }
            NES_DEBUG(channelName << ": Connection with server failed! Reconnecting attempt " << i << " backoff time "
                                  << std::to_string(backOffTime.count()));
            std::this_thread::sleep_for(backOffTime);// TODO make this async
            backOffTime *= 2;
            i++;
        }
        NES_ERROR(channelName << ": Error establishing a connection with server: " << channelId << " Closing socket!");
        zmqSocket.close();
        return nullptr;
    } catch (zmq::error_t& err) {
        if (err.num() == ETERM) {
            NES_DEBUG(channelName << ": Zmq context closed!");
        } else {
            NES_ERROR(channelName << ": Zmq error " << err.what());
            throw;
        }
        return nullptr;
    }
    return nullptr;
}

}// namespace detail

NetworkChannel::NetworkChannel(zmq::socket_t&& zmqSocket,
                               const ChannelId channelId,
                               std::string&& address,
                               Runtime::BufferManagerPtr bufferManager)
    : inherited(std::move(zmqSocket), channelId, std::move(address), std::move(bufferManager)) {
    NES_DEBUG("Initializing NetworkChannel " << channelId);
}

NetworkChannel::~NetworkChannel() { close(); }

void NetworkChannel::close() { inherited::close(canSendEvent && !canSendData); }

NetworkChannelPtr NetworkChannel::create(std::shared_ptr<zmq::context_t> const& zmqContext,
                                         std::string&& socketAddr,
                                         NesPartition nesPartition,
                                         ExchangeProtocol& protocol,
                                         Runtime::BufferManagerPtr bufferManager,
                                         std::chrono::milliseconds waitTime,
                                         uint8_t retryTimes) {
    return detail::createNetworkChannel<NetworkChannel>(zmqContext,
                                                        std::move(socketAddr),
                                                        nesPartition,
                                                        protocol,
                                                        bufferManager,
                                                        waitTime,
                                                        retryTimes);
}

EventOnlyNetworkChannel::EventOnlyNetworkChannel(zmq::socket_t&& zmqSocket,
                                                 const ChannelId channelId,
                                                 std::string&& address,
                                                 Runtime::BufferManagerPtr bufferManager)
    : inherited(std::move(zmqSocket), channelId, std::move(address), std::move(bufferManager)) {
    NES_DEBUG("Initializing EventOnlyNetworkChannel " << channelId);
}

EventOnlyNetworkChannel::~EventOnlyNetworkChannel() { close(); }

void EventOnlyNetworkChannel::close() { inherited::close(canSendEvent && !canSendData); }

EventOnlyNetworkChannelPtr EventOnlyNetworkChannel::create(std::shared_ptr<zmq::context_t> const& zmqContext,
                                                           std::string&& socketAddr,
                                                           NesPartition nesPartition,
                                                           ExchangeProtocol& protocol,
                                                           Runtime::BufferManagerPtr bufferManager,
                                                           std::chrono::milliseconds waitTime,
                                                           uint8_t retryTimes) {
    return detail::createNetworkChannel<EventOnlyNetworkChannel>(zmqContext,
                                                                 std::move(socketAddr),
                                                                 nesPartition,
                                                                 protocol,
                                                                 bufferManager,
                                                                 waitTime,
                                                                 retryTimes);
}

}// namespace NES::Network