#include <Network/NetworkCommon.hpp>
#include <Network/NetworkMessage.hpp>
#include <Network/OutputChannel.hpp>
#include <Network/ZmqUtils.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>

namespace NES {
namespace Network {

void OutputChannel::init(const std::string& socketAddr) {
    int linger = -1;
    uint64_t identity[] = {queryId, operatorId, partitionId, subpartitionId}; // 32 bytes
    try {
        zmqSocket.setsockopt(ZMQ_LINGER, &linger, sizeof(int));
        zmqSocket.setsockopt(ZMQ_IDENTITY, &identity, sizeof(identity));
        zmqSocket.connect(socketAddr);

        // send announcement to server to register channel
        sendMessage<Messages::ClientAnnounceMessage>(zmqSocket, queryId, operatorId, partitionId, subpartitionId);

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
                if (!(serverReadyMsg->getQueryId() == queryId &&
                    serverReadyMsg->getOperatorId() == operatorId &&
                    serverReadyMsg->getPartitionId() == partitionId &&
                    serverReadyMsg->getSubpartitionId() == subpartitionId)) {
                    NES_ERROR("[OutputChannel] Connection failed with server "
                                  << socketAddr << " for queryId=" << queryId << " operatorId=" << operatorId
                                  << " partitionId=" << partitionId << " subpartitionId=" << subpartitionId);
                    throw std::runtime_error("[OutputChannel] Wrong server ready message");
                }
                NES_INFO("[OutputChannel] Connection established with server "
                             << socketAddr << " for queryId=" << queryId << " operatorId=" << operatorId
                             << " partitionId=" << partitionId << " subpartitionId=" << subpartitionId);
                break;
            }
            default: {
                // got a wrong message type!
                NES_ERROR("OutputChannel: received unknown message " << recvHeader->getMsgType());
                throw std::runtime_error("[OutputChannel] Wrong message received");
            }
        }

    } catch (zmq::error_t& err) {
        // TODO check and log
        // TODO rethrow
    }
}

void OutputChannel::sendBuffer(TupleBuffer& inputBuffer) {
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
    NES_DEBUG("[OutputChannel] Sending buffer for queryId="
                  << queryId << " operatorId=" << operatorId << " partitionId=" << partitionId << " subpartitionId="
                  << subpartitionId << " with " << inputBuffer.getNumberOfTuples() << "/"
                  << inputBuffer.getBufferSize());

    zmqSocket.send(sendHeader, kSendMore);
    zmqSocket.send(sendBufferHeader, kSendMore);
    zmqSocket.send(sendBuffer);
}

void OutputChannel::onError() {}

void OutputChannel::close() {
    if (isClosed) {
        return;
    }
    sendMessage<Messages::EndOfStreamMessage>(zmqSocket, queryId, operatorId, partitionId, subpartitionId);
    zmqSocket.close();
    NES_DEBUG("[OutputChannel] Socket closed for queryId=" << queryId << " operatorId=" << operatorId << " partitionId="
                                                           << partitionId << " subpartitionId=" << subpartitionId);
    isClosed = true;
}

} // namespace Network
} // namespace NES