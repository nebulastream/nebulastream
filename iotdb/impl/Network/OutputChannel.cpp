#include <Network/OutputChannel.hpp>
#include <Network/ZmqUtils.hpp>
#include <Util/Logger.hpp>

namespace NES {
namespace Network {

void OutputChannel::init(const std::string& socketAddr) {
    int linger = -1;
    uint32_t identity[] = {queryId, operatorId, partitionId, subpartitionId}; // 16 bytes
    try {
        zmqSocket.setsockopt(ZMQ_LINGER, &linger, sizeof(int));
        zmqSocket.setsockopt(ZMQ_IDENTITY, &identity, sizeof(identity));
        zmqSocket.connect(socketAddr);

        sendMessage<Messages::ClientAnnounceMessage>(zmqSocket, queryId, operatorId, partitionId, subpartitionId);

        zmq::message_t recvHeaderMsg;
        zmqSocket.recv(recvHeaderMsg);

        auto recvHeader = recvHeaderMsg.data<Messages::MessageHeader>();

        if (recvHeader->getMagicNumber() != Messages::NES_NETWORK_MAGIC_NUMBER) {
            // TODO document here
            throw std::exception();
        }
        switch (recvHeader->getMsgType()) {
            case Messages::kServerReady: {
                zmq::message_t recvMsg;
                zmqSocket.recv(&recvMsg);
                auto serverReadyMsg = recvMsg.data<Messages::ServerReadyMessage>();
                if (!(serverReadyMsg->getQueryId() == queryId &&
                    serverReadyMsg->getOperatorId() == operatorId &&
                    serverReadyMsg->getPartitionId() == partitionId &&
                    serverReadyMsg->getSubpartitionId() == subpartitionId)) {
                    NES_DEBUG("[OutputChannel] Connection failed with server " << socketAddr
                        << " for queryId=" << queryId << " operatorId=" << operatorId << " partitionId=" << partitionId << " subpartitionId=" << subpartitionId);
                    // TODO error handling
                    throw std::runtime_error("[OutputChannel] Wrong server ready message");
                }
                NES_DEBUG("[OutputChannel] Connection established with server " << socketAddr
                << " for queryId=" << queryId << " operatorId=" << operatorId << " partitionId=" << partitionId << " subpartitionId=" << subpartitionId);
                break;
            }
            default: {
                // got a wrong message type!
                // TODO document here
                NES_ERROR("OutputChannel: received unknown message " <<  recvHeader->getMsgType());
                throw std::runtime_error("[OutputChannel] Wrong message received");
            }
        }

    } catch (zmq::error_t& err) {
        // TODO check and log
        // TODO rethrow
    }
}

void OutputChannel::sendBuffer() {

}

void OutputChannel::onError() {

}

void OutputChannel::close() {
    if (isClosed) {
        return;
    }
    sendMessage<Messages::EndOfStreamMessage>(zmqSocket, queryId, operatorId, partitionId, subpartitionId);
    zmqSocket.close();
    NES_DEBUG("[OutputChannel] Socket closed for queryId=" << queryId << " operatorId=" << operatorId << " partitionId=" << partitionId << " subpartitionId=" << subpartitionId);
    isClosed = true;
}

}
}