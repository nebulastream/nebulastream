#include <Network/NetworkCommon.hpp>
#include <Network/NetworkMessage.hpp>
#include <Network/OutputChannel.hpp>
#include <Network/ZmqUtils.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <NodeEngine/detail/TupleBufferImpl.hpp>

namespace NES {
namespace Network {

OutputChannel::OutputChannel(std::shared_ptr<zmq::context_t> zmqContext,
                             const std::string& address,
                             NesPartition nesPartition,
                             uint8_t waitTime, uint8_t retryTimes,
                             std::function<void(Messages::ErroMessage)> onError) : socketAddr(address),
                                                                                   zmqSocket(*zmqContext, ZMQ_DEALER),
                                                                                   nesPartition(nesPartition),
                                                                                   isClosed(false),
                                                                                   connected(false),
                                                                                   onErrorCb(std::move(onError)) {
    init(waitTime, retryTimes);
}

void OutputChannel::init(u_int64_t waitTime, u_int64_t retryTimes) {
    int linger = -1;
    try {
        zmqSocket.setsockopt(ZMQ_LINGER, &linger, sizeof(int));
        zmqSocket.setsockopt(ZMQ_IDENTITY, &nesPartition, sizeof(NesPartition));
        zmqSocket.connect(socketAddr);
        int i = 0;

        while ((!connected) && (i <= retryTimes)) {
            if (i > 0) {
                sleep(waitTime);
                NES_INFO("OutputChannel: Connection with server failed! Reconnecting attempt " << i);
            }
            i = i + 1;
            connected = registerAtServer();
        }
    } catch (zmq::error_t& err) {
        if (err.num() == ETERM) {
            NES_DEBUG("OutputChannel: Zmq context closed!");
        } else {
            NES_ERROR("OutputChannel: Zmq error " << err.what());
            throw err;
        }
    }
    if (!connected) {
        NES_ERROR("OutputChannel: Error establishing a connection with server. Closing socket!");
        close();
    }
}

bool OutputChannel::registerAtServer() {
    // send announcement to server to register channel
    sendMessage<Messages::ClientAnnounceMessage>(zmqSocket, nesPartition);

    zmq::message_t recvHeaderMsg;
    zmqSocket.recv(&recvHeaderMsg);

    auto recvHeader = recvHeaderMsg.data<Messages::MessageHeader>();

    if (recvHeader->getMagicNumber() != Messages::NES_NETWORK_MAGIC_NUMBER) {
        NES_ERROR("OutputChannel: Message from server is corrupt!");
        //TODO: think if it makes sense to reconnect after this error
        return false;
    }

    switch (recvHeader->getMsgType()) {
        case Messages::ServerReady: {
            zmq::message_t recvMsg;
            zmqSocket.recv(&recvMsg);
            auto serverReadyMsg = recvMsg.data<Messages::ServerReadyMessage>();
            // check if server responds with a ServerReadyMessage
            // check if the server has the correct corresponding channel registered, this is guaranteed by matching IDs
            if (!(serverReadyMsg->getNesPartition() == nesPartition)) {
                NES_ERROR("OutputChannel: Connection failed with server "
                          << socketAddr << " for " << nesPartition.toString()
                          << "->Wrong server ready message! Reason: Partitions are not matching");
                return false;
            }
            NES_INFO("OutputChannel: Connection established with server " << socketAddr << " for "
                                                                          << nesPartition.toString());
            return true;
        }
        case Messages::ErrorMessage: {
            // if server receives a message that an error occured
            zmq::message_t errorEnvelope;
            zmqSocket.recv(&errorEnvelope);
            auto errorMsg = errorEnvelope.data<Messages::ErroMessage>();

            NES_ERROR("OutputChannel: Received error from server-> " << errorMsg->getErrorTypeAsString());
            onError(*errorMsg);
            return false;
        }
        default: {
            // got a wrong message type!
            NES_ERROR("OutputChannel: received unknown message " << recvHeader->getMsgType());
            return false;
        }
    }
}

bool OutputChannel::sendBuffer(TupleBuffer& inputBuffer) {
    auto bufferSize = inputBuffer.getBufferSize();
    auto tupleSize = inputBuffer.getTupleSizeInBytes();
    auto numOfTuples = inputBuffer.getNumberOfTuples();
    auto payloadSize = tupleSize * numOfTuples;
    auto ptr = inputBuffer.getBuffer<uint8_t>();
    auto bufferSizeAsVoidPointer = reinterpret_cast<void*>(bufferSize);// DON'T TRY THIS AT HOME :P
    sendMessage<Messages::DataBufferMessage, kSendMore>(zmqSocket, payloadSize, numOfTuples);
    inputBuffer.retain();
    size_t sentBytes = zmqSocket.send(zmq::message_t(ptr, payloadSize, &detail::zmqBufferRecyclingCallback, bufferSizeAsVoidPointer));
    if (sentBytes>0) {
        //NES_DEBUG("OutputChannel: Sending buffer for " << nesPartition.toString() << " with "
        //                                               << inputBuffer.getNumberOfTuples() << "/"
        //                                               << inputBuffer.getBufferSize());
        return true;
    }
    NES_ERROR("OutputChannel: Error sending buffer for " << nesPartition.toString());
    return false;
}

void OutputChannel::onError(Messages::ErroMessage& errorMsg) {
    onErrorCb(errorMsg);
}

void OutputChannel::close() {
    if (isClosed) {
        return;
    }
    if (connected) {
        sendMessage<Messages::EndOfStreamMessage>(zmqSocket, nesPartition);
    }

    zmqSocket.close();
    NES_DEBUG("OutputChannel: Socket closed for " << nesPartition.toString());
    isClosed = true;
    connected = false;
}

bool OutputChannel::isConnected() const {
    return connected;
}

}// namespace Network
}// namespace NES