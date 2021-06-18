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

#include <Network/ExchangeProtocol.hpp>
#include <Network/NetworkMessage.hpp>
#include <Network/OutputChannel.hpp>
#include <Network/ZmqUtils.hpp>
#include <Runtime/NesThread.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/detail/TupleBufferImpl.hpp>
#include <Util/Logger.hpp>

namespace NES::Network {

OutputChannel::OutputChannel(zmq::socket_t&& zmqSocket, const ChannelId channelId, std::string&& address)
    : socketAddr(std::move(address)), zmqSocket(std::move(zmqSocket)), channelId(channelId) {
    NES_DEBUG("OutputChannel: Initializing OutputChannel " << channelId);
}

std::unique_ptr<OutputChannel> OutputChannel::create(std::shared_ptr<zmq::context_t> const& zmqContext,
                                                     std::string&& socketAddr,
                                                     QuerySubPlanId querySubPlanId,
                                                     NesPartition nesPartition,
                                                     ExchangeProtocol& protocol,
                                                     std::chrono::seconds waitTime,
                                                     uint8_t retryTimes) {
    std::chrono::seconds backOffTime = waitTime;
    try {
        ChannelId channelId(querySubPlanId, nesPartition, Runtime::NesThread::getId());
        zmq::socket_t zmqSocket(*zmqContext, ZMQ_DEALER);
        constexpr int linger = -1;
        NES_DEBUG("OutputChannel: Connecting with zmq-socketopt linger=" << std::to_string(linger) << ", id=" << channelId);
        zmqSocket.set(zmq::sockopt::linger, linger);
        //zmqSocket.setsockopt(ZMQ_IDENTITY, &channelId, sizeof(ChannelId));
        zmqSocket.set(zmq::sockopt::routing_id, zmq::const_buffer{&channelId, sizeof(ChannelId)});
        zmqSocket.connect(socketAddr);
        int i = 0;

        while (i < retryTimes) {
            sendMessage<Messages::ClientAnnounceMessage>(zmqSocket, channelId);

            zmq::message_t recvHeaderMsg;
            auto optRecvStatus = zmqSocket.recv(recvHeaderMsg, kZmqRecvDefault);
            NES_ASSERT2_FMT(optRecvStatus.has_value(), "invalid recv");

            auto* recvHeader = recvHeaderMsg.data<Messages::MessageHeader>();

            if (recvHeader->getMagicNumber() != Messages::NES_NETWORK_MAGIC_NUMBER) {
                NES_THROW_RUNTIME_ERROR("OutputChannel: Message from server is corrupt!");
            }

            switch (recvHeader->getMsgType()) {
                case Messages::kServerReady: {
                    zmq::message_t recvMsg;
                    auto optRecvStatus2 = zmqSocket.recv(recvMsg, kZmqRecvDefault);
                    NES_ASSERT2_FMT(optRecvStatus2.has_value(), "invalid recv");
                    auto* serverReadyMsg = recvMsg.data<Messages::ServerReadyMessage>();
                    // check if server responds with a ServerReadyMessage
                    // check if the server has the correct corresponding channel registered, this is guaranteed by matching IDs
                    if (!(serverReadyMsg->getChannelId().getNesPartition() == channelId.getNesPartition())) {
                        NES_ERROR("OutputChannel: Connection failed with server "
                                  << socketAddr << " for " << channelId.getNesPartition().toString()
                                  << "->Wrong server ready message! Reason: Partitions are not matching");
                        break;
                    }

                    if (serverReadyMsg->isOk() && !serverReadyMsg->isPartitionNotFound()) {
                        NES_INFO("OutputChannel: Connection established with server " << socketAddr << " for " << channelId);
                        return std::make_unique<OutputChannel>(std::move(zmqSocket), channelId, std::move(socketAddr));
                    }
                    protocol.onChannelError(Messages::ErrorMessage(channelId, serverReadyMsg->getErrorType()));
                    break;
                }
                case Messages::kErrorMessage: {
                    // if server receives a message that an error occured
                    zmq::message_t errorEnvelope;
                    auto optRecvStatus3 = zmqSocket.recv(errorEnvelope, kZmqRecvDefault);
                    NES_ASSERT2_FMT(optRecvStatus3.has_value(), "invalid recv");
                    auto errorMsg = *errorEnvelope.data<Messages::ErrorMessage>();
                    NES_ERROR("OutputChannel: Received error from server-> " << errorMsg.getErrorTypeAsString());
                    protocol.onChannelError(errorMsg);
                    break;
                }
                default: {
                    // got a wrong message type!
                    NES_ERROR("OutputChannel: received unknown message " << recvHeader->getMsgType());
                    return nullptr;
                }
            }
            std::this_thread::sleep_for(backOffTime);
            backOffTime *= 2;
            NES_INFO("OutputChannel: Connection with server failed! Reconnecting attempt " << i);
            i++;
        }
        NES_ERROR("OutputChannel: Error establishing a connection with server: " << channelId << " Closing socket!");
        zmqSocket.close();
        return nullptr;
    } catch (zmq::error_t& err) {
        if (err.num() == ETERM) {
            NES_DEBUG("OutputChannel: Zmq context closed!");
        } else {
            NES_ERROR("OutputChannel: Zmq error " << err.what());
            throw;
        }
    }
    return nullptr;
}

bool OutputChannel::sendBuffer(Runtime::TupleBuffer& inputBuffer, uint64_t tupleSize) {
    auto numOfTuples = inputBuffer.getNumberOfTuples();
    auto originId = inputBuffer.getOriginId();
    auto watermark = inputBuffer.getWatermark();
    auto sequenceNumber = inputBuffer.getSequenceNumber();
    auto creationTimestamp = inputBuffer.getCreationTimestamp();
    auto payloadSize = tupleSize * numOfTuples;
    auto* ptr = inputBuffer.getBuffer<uint8_t>();
    if (payloadSize == 0) {
        return true;
    }
    sendMessage<Messages::DataBufferMessage, kZmqSendMore>(zmqSocket,
                                                           payloadSize,
                                                           numOfTuples,
                                                           originId,
                                                           watermark,
                                                           creationTimestamp,
                                                           sequenceNumber);

    // We need to retain the `inputBuffer` here, because the send function operates asynchronously and we therefore
    // need to pass the responsibility of freeing the tupleBuffer instance to ZMQ's callback.
    inputBuffer.retain();
    auto const sentBytesOpt = zmqSocket.send(
        zmq::message_t(ptr, payloadSize, &Runtime::detail::zmqBufferRecyclingCallback, inputBuffer.getControlBlock()),
        kZmqSendDefault);
    if (sentBytesOpt.has_value()) {
        NES_TRACE("OutputChannel: Sending buffer with " << inputBuffer.getNumberOfTuples() << "/" << inputBuffer.getBufferSize()
                                                        << "-" << inputBuffer.getOriginId());
        return true;
    }
    NES_ERROR("OutputChannel: Error sending buffer for " << channelId);
    return false;
}

void OutputChannel::sendReconfigurationMessage(QueryReconfigurationPlanPtr queryReconfigurationPlan) {
    NES_DEBUG("OutputChannel::sendReconfigurationMessage: sending reconfiguration message ("
              << queryReconfigurationPlan << ") on channel (" << channelId << ").");
    std::string serializedReconfigurationPlan = queryReconfigurationPlan->serializeToString();
    auto sz = serializedReconfigurationPlan.size();
    sendMessage<Messages::QueryReconfigurationMessage, kZmqSendMore>(zmqSocket, channelId, sz);
    zmq::message_t query = zmq::message_t(serializedReconfigurationPlan.c_str(), sz);
    zmqSocket.send(query, kZmqSendDefault);
}

void OutputChannel::onError(Messages::ErrorMessage& errorMsg) { NES_ERROR(errorMsg.getErrorTypeAsString()); }

void OutputChannel::close(bool notifyRelease) {
    if (isClosed) {
        return;
    }
    if (notifyRelease) {
        sendMessage<Messages::EndOfStreamMessage>(zmqSocket, channelId);
    }
    zmqSocket.close();
    NES_DEBUG("OutputChannel: Socket closed for " << channelId);
    isClosed = true;
}

}// namespace NES::Network