#ifndef NES_OUTPUTCHANNEL_HPP
#define NES_OUTPUTCHANNEL_HPP

#include <boost/core/noncopyable.hpp>
#include <memory>
#include <Network/NetworkMessage.hpp>
#include <iostream>

namespace NES {
namespace Network {

/**
 * NOT THREAD SAFE! DON'T SHARE AMONG THREADS!
 *
 */
class OutputChannel : public boost::noncopyable {
public:

    explicit OutputChannel(
            std::shared_ptr<zmq::context_t> zmqContext,
            QueryId queryId,
            OperatorId operatorId,
            PartitionId partitionId,
            SubpartitionId subpartitionId,
            const std::string& address)
            :   zmqSocket(*zmqContext, ZMQ_DEALER),
                queryId(queryId),
                operatorId(operatorId),
                partitionId(partitionId),
                subpartitionId(subpartitionId),
                isClosed(false) {
        init(address);
    }

    ~OutputChannel() {
        close();
    }

private:
    void init(const std::string& socketAddr) {
        int linger = -1;
        uint32_t identity[] = { queryId, operatorId, partitionId, subpartitionId }; // 16 bytes
        try {
            zmqSocket.setsockopt(ZMQ_LINGER, &linger, sizeof(int));
            zmqSocket.setsockopt(ZMQ_IDENTITY, &identity, sizeof(identity));
            zmqSocket.connect(socketAddr);

            Messages::MessageHeader header(Messages::kClientAnnouncement, sizeof(Messages::ClientAnnounceMessage));
            Messages::ClientAnnounceMessage announceMessage(queryId, operatorId, partitionId, subpartitionId);
            zmq::message_t sendHeader(&header, sizeof(Messages::MessageHeader));
            zmq::message_t sendMsg(&announceMessage, sizeof(Messages::ClientAnnounceMessage));
            zmqSocket.send(sendHeader, zmq::send_flags::sndmore);
            zmqSocket.send(sendMsg, zmq::send_flags::none);

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
//                        NES_ERROR("OutputChannel: expected " << queryId << " " << operatorId << " " << partitionId << " " << subpartitionId << " but received "
//                            << serverReadyMsg->getQueryId() << serverReadyMsg->getQueryId() << " " << serverReadyMsg->getOperatorId() << " "
//                            << serverReadyMsg->getPartitionId() << " " << serverReadyMsg->getSubpartitionId());
                        throw std::runtime_error("[OutputChannel] Wrong server ready message");
                    }
                    break;
                }
                default: {
                    // got a wrong message type!
                    // TODO document here
//                    NES_ERROR("OutputChannel: received " <<  recvHeader->getMsgType());
                    throw std::runtime_error("[OutputChannel] Wrong message received");
                }
            }

        } catch (zmq::error_t& err) {
            // TODO check and log
            // TODO rethrow
        }
    }

public:
    void sendBuffer(/** tuple buffer, num of records **/) {

    }

    void onError(/** error info **/) {

    }

    void close() {
        if (isClosed) {
            return;
        }

        Messages::MessageHeader header(Messages::kEndOfStream, sizeof(Messages::EndOfStreamMessage));
        Messages::EndOfStreamMessage msgEndOfStream(queryId, operatorId, partitionId, subpartitionId);

        zmq::message_t headerEnvelope(&header, sizeof(Messages::EndOfStreamMessage));
        zmq::message_t endOfMessageEnvelope(&msgEndOfStream, sizeof(Messages::EndOfStreamMessage));

        zmqSocket.send(headerEnvelope, zmq::send_flags::sndmore);
        zmqSocket.send(endOfMessageEnvelope, zmq::send_flags::none);

        zmqSocket.close();

        isClosed = true;
    }


private:
    zmq::socket_t zmqSocket;
    const QueryId queryId;
    const OperatorId operatorId;
    const PartitionId partitionId;
    const SubpartitionId subpartitionId;

    bool isClosed;

};
}
}






#endif //NES_OUTPUTCHANNEL_HPP
