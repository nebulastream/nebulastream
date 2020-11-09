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
#include <NodeEngine/NesThread.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <NodeEngine/detail/TupleBufferImpl.hpp>
#include <Util/Logger.hpp>

namespace NES {
namespace Network {

OutputChannel::OutputChannel(zmq::socket_t&& zmqSocket, const ChannelId channelId, const std::string address)
    : zmqSocket(std::move(zmqSocket)), channelId(channelId), socketAddr(address), isClosed(false) {
    NES_DEBUG("OutputChannel: Initializing OutputChannel " << channelId);
}

std::unique_ptr<OutputChannel> OutputChannel::create(
    std::shared_ptr<zmq::context_t> zmqContext,
    const std::string socketAddr,
    NesPartition nesPartition,
    ExchangeProtocol& protocol,
    std::chrono::seconds waitTime,
    uint8_t retryTimes) {
    int linger = -1;
    try {
        ChannelId channelId(nesPartition, NesThread::getId());
        zmq::socket_t zmqSocket(*zmqContext, ZMQ_DEALER);
        NES_DEBUG("OutputChannel: Connecting with zmq-socketopt linger=" << linger << ", id=" << channelId);
        zmqSocket.setsockopt(ZMQ_LINGER, &linger, sizeof(int));
        zmqSocket.setsockopt(ZMQ_IDENTITY, &channelId, sizeof(ChannelId));
        zmqSocket.connect(socketAddr);
        int i = 0;

        while (i < retryTimes) {
            sendMessage<Messages::ClientAnnounceMessage>(zmqSocket, channelId);

            zmq::message_t recvHeaderMsg;
            zmqSocket.recv(&recvHeaderMsg);

            auto recvHeader = recvHeaderMsg.data<Messages::MessageHeader>();

            if (recvHeader->getMagicNumber() != Messages::NES_NETWORK_MAGIC_NUMBER) {
                NES_THROW_RUNTIME_ERROR("OutputChannel: Message from server is corrupt!");
            }

            switch (recvHeader->getMsgType()) {
                case Messages::kServerReady: {
                    zmq::message_t recvMsg;
                    zmqSocket.recv(&recvMsg);
                    auto serverReadyMsg = recvMsg.data<Messages::ServerReadyMessage>();
                    // check if server responds with a ServerReadyMessage
                    // check if the server has the correct corresponding channel registered, this is guaranteed by matching IDs
                    if (!(serverReadyMsg->getChannelId().getNesPartition() == channelId.getNesPartition())) {
                        NES_ERROR("OutputChannel: Connection failed with server "
                                  << socketAddr << " for " << channelId.getNesPartition().toString()
                                  << "->Wrong server ready message! Reason: Partitions are not matching");
                        break;
                    }

                    if (serverReadyMsg->isOk() && !serverReadyMsg->isPartitionNotFound()) {
                        NES_INFO("OutputChannel: Connection established with server " << socketAddr << " for "
                                                                                      << channelId);
                        return std::make_unique<OutputChannel>(std::move(zmqSocket), channelId, socketAddr);
                    }
                    protocol.onChannelError(Messages::ErrorMessage(channelId, serverReadyMsg->getErrorType()));
                    break;
                }
                case Messages::kErrorMessage: {
                    // if server receives a message that an error occured
                    zmq::message_t errorEnvelope;
                    zmqSocket.recv(&errorEnvelope);
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
            std::this_thread::sleep_for(waitTime);
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
            throw err;
        }
    }
    return nullptr;
}

bool OutputChannel::sendBuffer(TupleBuffer& inputBuffer, size_t tupleSize) {
    auto bufferSize = inputBuffer.getBufferSize();
    auto numOfTuples = inputBuffer.getNumberOfTuples();
    auto originId = inputBuffer.getOriginId();
    auto payloadSize = tupleSize * numOfTuples;
    auto ptr = inputBuffer.getBuffer<uint8_t>();
    auto bufferSizeAsVoidPointer = reinterpret_cast<void*>(bufferSize);// DON'T TRY THIS AT HOME :P
    if (payloadSize == 0) {
        return true;
    }
    sendMessage<Messages::DataBufferMessage, kSendMore>(zmqSocket, payloadSize, numOfTuples, originId);
    inputBuffer.retain();
    auto sentBytesOpt =
        zmqSocket.send(zmq::message_t(ptr, payloadSize, &detail::zmqBufferRecyclingCallback, bufferSizeAsVoidPointer), zmq::send_flags::none);
    if (sentBytesOpt.has_value()) {
        //NES_DEBUG("OutputChannel: Sending buffer for " << nesPartition.toString() << " with "
        //                                               << inputBuffer.getNumberOfTuples() << "/"
        //                                               << inputBuffer.getBufferSize());
        return true;
    }
    NES_ERROR("OutputChannel: Error sending buffer for " << channelId);
    return false;
}

void OutputChannel::onError(Messages::ErrorMessage& errorMsg) {
    NES_ERROR(errorMsg.getErrorTypeAsString());
}

void OutputChannel::close() {
    if (isClosed) {
        return;
    }
    sendMessage<Messages::EndOfStreamMessage>(zmqSocket, channelId);
    zmqSocket.close();
    NES_DEBUG("OutputChannel: Socket closed for " << channelId);
    isClosed = true;
}

}// namespace Network
}// namespace NES