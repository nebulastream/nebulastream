#ifndef NES_NETWORKMESSAGE_HPP
#define NES_NETWORKMESSAGE_HPP

#include <Network/NetworkCommon.hpp>
#include <cstdint>
#include <utility>

namespace NES {
namespace Network {
namespace Messages {

using nes_magic_number_t = uint64_t;
static constexpr nes_magic_number_t NES_NETWORK_MAGIC_NUMBER = 0xBADC0FFEE;

enum MessageType {
    ClientAnnouncement,
    ServerReady,
    DataBuffer,
    ErrorMessage,
    EndOfStream
};

enum ErrorType {
    PartitionNotRegisteredError,
    UnknownError
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
    static constexpr MessageType MESSAGE_TYPE = ClientAnnouncement;

    explicit ClientAnnounceMessage(QueryId queryId,
                                   OperatorId operatorId,
                                   PartitionId partitionId,
                                   SubpartitionId subpartitionId)
        : queryId(queryId),
          operatorId(operatorId),
          partitionId(partitionId),
          subpartitionId(subpartitionId) {}

    /**
     * @brief getter for the queryId
     * @return the queryId
     */
    QueryId getQueryId() const {
        return queryId;
    }

    /**
     * @brief getter for the operatorId
     * @return the operatorId
     */
    OperatorId getOperatorId() const {
        return operatorId;
    }

    /**
     * @brief getter for the partitionId
     * @return the partitionId
     */
    PartitionId getPartitionId() const {
        return partitionId;
    }

    /**
     * @brief getter for the getSubpartitionId
     * @return the subpartitionId
     */
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

    /**
     * @brief getter for the queryId
     * @return the queryId
     */
    QueryId getQueryId() const {
        return queryId;
    }

    /**
     * @brief getter for the operatorId
     * @return the operatorId
     */
    OperatorId getOperatorId() const {
        return operatorId;
    }

    /**
     * @brief getter for the partitionId
     * @return the partitionId
     */
    PartitionId getPartitionId() const {
        return partitionId;
    }

    /**
     * @brief getter for the getSubpartitionId
     * @return the subpartitionId
     */
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

    /**
     * @brief get the payloadSize of the BufferMessage
     * @return the payloadSize
     */
    const uint32_t getPayloadSize() const {
        return payloadSize;
    }

    /**
     * @brief get the number of records within the current BufferMessage
     * @return the number of records
     */
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
    explicit ErroMessage(ErrorType error) : error(error) {};

    const ErrorType getErrorType() const { return error; }

    const std::string getErrorTypeAsString() const {
        if (error == ErrorType::PartitionNotRegisteredError) {
            return "PartitionNotRegisteredError";
        } else {
            return "UnknownError";
        }
    }

  private:
    const ErrorType error;
};

}// namespace Messages
}// namespace Network
}// namespace NES

#endif//NES_NETWORKMESSAGE_HPP
