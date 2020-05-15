#include <Network/NetworkCommon.hpp>
#include <Network/NetworkMessage.hpp>
#include <Network/OutputChannel.hpp>
#include <Network/ZmqUtils.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <utility>

namespace NES {
namespace Network {

bool OutputChannel::init() {
    int linger = -1;
    uint64_t identity[] = {nesPartition.getQueryId(), nesPartition.getOperatorId(),
                           nesPartition.getPartitionId(), nesPartition.getSubpartitionId()};// 32 bytes
    try {
        zmqSocket.setsockopt(ZMQ_LINGER, &linger, sizeof(int));
        zmqSocket.setsockopt(ZMQ_IDENTITY, &identity, sizeof(identity));
        zmqSocket.connect(socketAddr);

        // send announcement to server to register channel
        sendMessage<Messages::ClientAnnounceMessage>(zmqSocket, nesPartition.getQueryId(), nesPartition.getOperatorId(),
                                                     nesPartition.getPartitionId(), nesPartition.getSubpartitionId());

        zmq::message_t recvHeaderMsg;
        zmqSocket.recv(&recvHeaderMsg);

        auto recvHeader = recvHeaderMsg.data<Messages::MessageHeader>();

        if (recvHeader->getMagicNumber() != Messages::NES_NETWORK_MAGIC_NUMBER) {
            // TODO document here
            throw std::exception();
        }
        switch (recvHeader->getMsgType()) {
            case Messages::ServerReady: {
                zmq::message_t recvMsg;
                zmqSocket.recv(&recvMsg);
                auto serverReadyMsg = recvMsg.data<Messages::ServerReadyMessage>();
                // check if server responds with a ServerReadyMessage
                // check if the server has the correct corresponding channel registered, this is guaranteed by matching IDs
                if (!(serverReadyMsg->getQueryId() == nesPartition.getQueryId()
                    && serverReadyMsg->getOperatorId() == nesPartition.getOperatorId()
                    && serverReadyMsg->getPartitionId() == nesPartition.getPartitionId()
                    && serverReadyMsg->getSubpartitionId() == nesPartition.getSubpartitionId())) {
                    NES_ERROR("OutputChannel: Connection failed with server "
                                  << socketAddr << " for " << nesPartition.toString());
                    throw std::runtime_error("OutputChannel: Wrong server ready message->Partitions are not matching");
                }
                NES_INFO("OutputChannel: Connection established with server "
                             << socketAddr << " for " << nesPartition.toString());
                return true;
            }
            case Messages::ErrorMessage: {
                // if server receives a message that an error occured
                zmq::message_t recvMsg;
                zmqSocket.recv(&recvMsg);
                auto errorMsg = recvMsg.data<Messages::ErroMessage>();

                NES_ERROR("OutputChannel: Received error from server-> " << errorMsg->getErrorTypeAsString());
                onError(*errorMsg);
                return false;
            }
            default: {
                // got a wrong message type!
                NES_ERROR("OutputChannel: received unknown message " << recvHeader->getMsgType());
                throw std::runtime_error("OutputChannel: Unknown received");
            }
        }
    } catch (zmq::error_t& err) {
        if (err.num() == ETERM) {
            NES_DEBUG("OutputChannel: Zmq context closed!");
        } else {
            NES_ERROR("OutputChannel: Zmq error " << err.what());
            throw err;
        }
    }
    return false;
}

bool OutputChannel::sendBuffer(TupleBuffer& inputBuffer) {
    try {
        // create a header message for MessageType
        Messages::MessageHeader header{Messages::DataBuffer, sizeof(Messages::DataBuffer)};
        zmq::message_t sendHeader(&header, sizeof(Messages::MessageHeader));

        // create the header for the buffer
        Messages::DataBufferMessage bufferHeader{static_cast<unsigned int>(inputBuffer.getBufferSize()),
                                                 static_cast<unsigned int>(inputBuffer.getNumberOfTuples())};
        zmq::message_t sendBufferHeader(&bufferHeader, sizeof(Messages::DataBufferMessage));

        // create the message with the buffer payload
        // TODO: in future change that buffer is sent without copying
        zmq::message_t sendBuffer(inputBuffer.getBuffer(), inputBuffer.getBufferSize());

        // send all messages in one shot
        NES_DEBUG("OutputChannel: Sending buffer for " << nesPartition.toString() << " with "
                                                       << inputBuffer.getNumberOfTuples() << "/"
                                                       << inputBuffer.getBufferSize());

        zmqSocket.send(sendHeader, kSendMore);
        zmqSocket.send(sendBufferHeader, kSendMore);
        zmqSocket.send(sendBuffer);
        return true;
    }
    catch (std::exception& ex) {
        NES_ERROR(ex.what());
        return false;
    }
}

void OutputChannel::onError(Messages::ErroMessage& errorMsg) {
    onErrorCb(errorMsg);
}

void OutputChannel::close() {
    if (isClosed) {
        return;
    }
    if (ready) {
        sendMessage<Messages::EndOfStreamMessage>(zmqSocket, nesPartition.getQueryId(), nesPartition.getOperatorId(),
                                                  nesPartition.getPartitionId(), nesPartition.getSubpartitionId());
    }

    zmqSocket.close();
    NES_DEBUG("OutputChannel: Socket closed for " << nesPartition.toString());
    isClosed = true;
    ready = false;
}

bool OutputChannel::isReady() {
    return ready;
}

}// namespace Network
}// namespace NES