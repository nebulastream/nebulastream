#ifndef NES_NETWORKMESSAGE_HPP
#define NES_NETWORKMESSAGE_HPP

#include <cstdint>
#include <Network/NetworkCommon.hpp>

namespace NES {
namespace Network {
namespace Messages {

    using nes_magic_number_t = uint32_t;
    static constexpr nes_magic_number_t NES_NETWORK_MAGIC_NUMBER = 0xBADC0FFEE;

    enum MessageType {
        kClientAnnouncement,
        kServerReady,
        kDataBuffer,
        kErrorMessage,
        kEndOfStream
    };

    class MessageHeader {
    public:
        explicit MessageHeader(MessageType msgType, uint32_t msgLength)
            : magicNumber(NES_NETWORK_MAGIC_NUMBER), msgType(msgType), msgLength(msgLength) {
        }

        nes_magic_number_t getMagicNumber() const {
            return magicNumber;
        }

        MessageType getMsgType() const {
            return msgType;
        }

        uint32_t getMsgLength() const {
            return msgLength;
        }

    private:
        const nes_magic_number_t magicNumber;
        const MessageType msgType;
        const uint32_t msgLength;
    };

    class ClientAnnounceMessage {
    public:
        explicit ClientAnnounceMessage(QueryId queryId,
                                       OperatorId operatorId,
                                       PartitionId partitionId,
                                       SubpartitionId subpartitionId)
                                       : queryId(queryId),
                                         operatorId(operatorId),
                                         partitionId(partitionId),
                                         subpartitionId(subpartitionId) {}
        QueryId getQueryId() const {
            return queryId;
        }

        OperatorId getOperatorId() const {
            return operatorId;
        }

        PartitionId getPartitionId() const {
            return partitionId;
        }

        SubpartitionId getSubpartitionId() const {
            return subpartitionId;
        }

    private:
        const QueryId queryId;
        const OperatorId operatorId;
        const PartitionId partitionId;
        const SubpartitionId subpartitionId;
    };

    class ServerReadyMessage {
    public:
        explicit ServerReadyMessage(
                QueryId queryId,
                OperatorId operatorId,
                PartitionId partitionId,
                SubpartitionId subpartitionId)
                : queryId(queryId),
                operatorId(operatorId),
                partitionId(partitionId),
                subpartitionId(subpartitionId) {}


        QueryId getQueryId() const {
            return queryId;
        }

        OperatorId getOperatorId() const {
            return operatorId;
        }

        PartitionId getPartitionId() const {
            return partitionId;
        }

        SubpartitionId getSubpartitionId() const {
            return subpartitionId;
        }

    private:
        const QueryId queryId;
        const OperatorId operatorId;
        const PartitionId partitionId;
        const SubpartitionId subpartitionId;
    };

    class DataBufferMessage {
    public:
    private:
        const QueryId queryId;
        const OperatorId operatorId;
        const PartitionId partitionId;
        const SubpartitionId subpartitionId;
        const uint32_t numOfRecords;
        const void* payload;
    };

    class EndOfStreamMessage {
    public:
        explicit EndOfStreamMessage(
            QueryId queryId,
            OperatorId operatorId,
            PartitionId partitionId,
            SubpartitionId subpartitionId)
            : queryId(queryId),
                operatorId(operatorId),
                partitionId(partitionId),
                subpartitionId(subpartitionId) {}
    private:
        const QueryId queryId;
        const OperatorId operatorId;
        const PartitionId partitionId;
        const SubpartitionId subpartitionId;
    };

    class ErroMessage {
    public:
    private:
    };


}
}
}

#endif //NES_NETWORKMESSAGE_HPP
