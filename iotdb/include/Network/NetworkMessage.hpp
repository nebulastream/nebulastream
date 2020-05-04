#ifndef NES_NETWORKMESSAGE_HPP
#define NES_NETWORKMESSAGE_HPP

#include <cstdint>
#include <Network/NetworkCommon.hpp>

namespace NES {
namespace Network {
namespace Messages {

using nes_magic_number_t = uint64_t;
static constexpr nes_magic_number_t NES_NETWORK_MAGIC_NUMBER = 0xBADC0FFEE;

enum MessageType { ClientAnnouncement, ServerReady, DataBuffer, ErrorMessage, EndOfStream };

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
    static constexpr MessageType MESSAGE_TYPE = ClientAnnouncement;

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
    static constexpr MessageType MESSAGE_TYPE = ServerReady;

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
    static constexpr MessageType MESSAGE_TYPE = DataBuffer;

    explicit DataBufferMessage(uint32_t payloadSize, uint32_t numOfRecords)
        : payloadSize(payloadSize), numOfRecords(numOfRecords) {

    }

    const uint32_t getPayloadSize() const {
        return payloadSize;
    }

    const uint32_t getNumOfRecords() const {
        return numOfRecords;
    }

  private:
    const uint32_t payloadSize;
    const uint32_t numOfRecords;

};

class EndOfStreamMessage {
  public:
    static constexpr MessageType MESSAGE_TYPE = EndOfStream;
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
    static constexpr MessageType MESSAGE_TYPE = ErrorMessage;
  private:
};

}
}
}

#endif //NES_NETWORKMESSAGE_HPP
