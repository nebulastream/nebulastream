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

OutputChannel::OutputChannel(zmq::socket_t&& zmqSocket, const ChannelId channelId, std::string&& address,std::queue<std::pair<Runtime::TupleBuffer, uint64_t>>&& buffer)
    : socketAddr(std::move(address)), zmqSocket(std::move(zmqSocket)), channelId(channelId), buffer(std::move(buffer)), buffering (false) {
    NES_DEBUG("OutputChannel: Initializing OutputChannel " << channelId);
}

std::unique_ptr<OutputChannel> OutputChannel::create(std::shared_ptr<zmq::context_t> const& zmqContext,
                                                     std::string&& socketAddr,
                                                     NesPartition nesPartition,
                                                     uint64_t nodeEngineId,
                                                     ExchangeProtocol& protocol,
                                                     std::chrono::seconds waitTime,
                                                     uint8_t retryTimes,
                                                     std::queue<std::pair<Runtime::TupleBuffer, uint64_t>>&& buffer) {
    std::chrono::seconds backOffTime = waitTime;
    try {
        ChannelId channelId(nesPartition,nodeEngineId);
        NES_DEBUG("ThreadId at OutputChannel: " << channelId.toString());
        zmq::socket_t zmqSocket(*zmqContext, ZMQ_DEALER);
        constexpr int linger = -1;
        //NES_ERROR("OutputChannel: Connecting with zmq-socketopt linger=" << std::to_string(linger) << ", id=" << channelId <<",address="<<socketAddr);
        zmqSocket.set(zmq::sockopt::linger, linger);
        //zmqSocket.setsockopt(ZMQ_IDENTITY, &channelId, sizeof(ChannelId));
        zmqSocket.set(zmq::sockopt::routing_id, zmq::const_buffer{&channelId, sizeof(ChannelId)});
        zmqSocket.connect(socketAddr);
        NES_ASSERT(zmqSocket.connected(), "Cannot connect to socket!");
        int i = 0;

        while (i < retryTimes) {
            NES_DEBUG("Sending Client Announcement to id= " << channelId);
            sendMessage<Messages::ClientAnnounceMessage>(zmqSocket, channelId);
            NES_DEBUG("OutputChannel: client announcement sent");

            zmq::message_t recvHeaderMsg;
            NES_DEBUG("Beginn receiving reply from ZMQ Socket");
            auto optRecvStatus = zmqSocket.recv(recvHeaderMsg, kZmqRecvDefault);
            NES_DEBUG("Reply received from ZMQ Socket");
            NES_ASSERT2_FMT(optRecvStatus.has_value(), "invalid recv");

            auto* recvHeader = recvHeaderMsg.data<Messages::MessageHeader>();

            if (recvHeader->getMagicNumber() != Messages::NES_NETWORK_MAGIC_NUMBER) {
                NES_DEBUG("Message from server is corrupt");
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
                        //NES_ERROR("OutputChannel: Connection established with server " << socketAddr << " for " << channelId);
                        return std::make_unique<OutputChannel>(std::move(zmqSocket), channelId, std::move(socketAddr), std::move(buffer));
                    }
                    protocol.onChannelError(Messages::ErrorMessage(channelId, serverReadyMsg->getErrorType()));
                    break;
                }
                case Messages::kErrorMessage: {
                    NES_ERROR("OutputChannel: Error on Client Announcement");
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
//    auto ts = std::chrono::system_clock::now();
//    auto time = std::chrono::duration_cast<std::chrono::nanoseconds>(ts.time_since_epoch()).count();
//    if(timeSinceLastBufferSent == 0){
//        NES_ERROR("OutputChannel: first buffer sent ");
//        timeSinceLastBufferSent = time;
//    }
//    else {
//        std::cout << "BST: " << (time - timeSinceLastBufferSent) <<std::endl;
//        timeSinceLastBufferSent = time;
//    }
    if(buffering){
        NES_ERROR("OutputChannel: Storing data intended for: " << socketAddr);
        buffer.push(std::pair<Runtime::TupleBuffer, uint64_t> {inputBuffer, tupleSize});
        NES_ERROR("Store size now: " <<  buffer.size());
        return true;
    }
    NES_DEBUG("OutputChannel: buffer sent");
    auto numOfTuples = inputBuffer.getNumberOfTuples();
    auto originId = inputBuffer.getOriginId();
    auto watermark = inputBuffer.getWatermark();
    auto sequenceNumber = inputBuffer.getSequenceNumber();
    auto creationTimestamp = inputBuffer.getCreationTimestamp();
    auto payloadSize = tupleSize * numOfTuples;
    auto* ptr = inputBuffer.getBuffer<uint8_t>();
    if (payloadSize == 0) {
        NES_DEBUG("payload size 0");
        return true;
    }
    NES_DEBUG("OutputChannel: Sending buffer to: " << socketAddr);
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

void OutputChannel::onError(Messages::ErrorMessage& errorMsg) { NES_ERROR(errorMsg.getErrorTypeAsString()); }

void OutputChannel::close() {
    if (isClosed) {
        return;
    }
    sendMessage<Messages::EndOfStreamMessage>(zmqSocket, channelId);
    zmqSocket.close();
    NES_DEBUG("OutputChannel: Socket closed for " << channelId);
    isClosed = true;
}

void OutputChannel::shutdownZMQSocket(bool withMessagePropagation) {
    if (isClosed) {
        return;
    }
    if(withMessagePropagation) {
        NES_DEBUG("OutputChannel: Sending RemoveQEP Message to " << channelId.toString() << " with address " << socketAddr);
        sendMessage<Messages::RemoveQEPMessage>(zmqSocket, channelId);
    }
    else{
        NES_DEBUG("OutputChannel: Sending DecrementPartitionCounter to "<< channelId.toString() << " with address " << socketAddr);
        sendMessage<Messages::DecrementPartitionCounterMessage>(zmqSocket, channelId);
    }
    zmqSocket.close();
    NES_DEBUG("OutputChannel: Socket closed for " << channelId);
    isClosed = true;
}
void OutputChannel::setBuffer(bool b) {
    buffering = b;
}
bool OutputChannel::isBuffering() { return buffering;}

std::queue<std::pair<Runtime::TupleBuffer, uint64_t>>&& OutputChannel::moveBuffer() {
    return std::move(buffer);
}
bool OutputChannel::emptyBuffer() {
    NES_DEBUG("OutputChannel: Emptying store intended for: " << socketAddr);
    if(buffer.empty()){
        NES_ERROR("OutputChannel: Store is empty!");
        buffering = false;
        return true;
    }
    NES_ERROR("OutputChannel: emptying store. Nr of elements: " << buffer.size());
    while(!buffer.empty()){
        auto data = buffer.front();
        if(sendBuffer( data.first, data.second)){
            buffer.pop();
            NES_DEBUG("OutputChannel: Successfully sent a stored TupleBuffer");
            continue;
        }
        NES_ERROR("OutputChannel: Error while senidng stored TupleBuffer");

    }
    NES_ERROR("OutputChannel: Store is empty");
    buffering = false;
    return true;
}

}// namespace NES::Network