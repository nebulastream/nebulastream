#ifndef NES_NETWORKMESSAGE_HPP
#define NES_NETWORKMESSAGE_HPP

#include <Network/ChannelId.hpp>
#include <Network/NesPartition.hpp>

#include <cstdint>
#include <stdexcept>
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

class ExchangeMessage {
  public:
    explicit ExchangeMessage(ChannelId channelId) : channelId(channelId) {}

    const ChannelId& getChannelId() const {
        return channelId;
    }

  private:
    const ChannelId channelId;
};

class ClientAnnounceMessage : public ExchangeMessage {
  public:
    static constexpr MessageType MESSAGE_TYPE = ClientAnnouncement;

    explicit ClientAnnounceMessage(ChannelId channelId) : ExchangeMessage(channelId) {
    }
};

class ServerReadyMessage : public ExchangeMessage {
  public:
    static constexpr MessageType MESSAGE_TYPE = ServerReady;

    explicit ServerReadyMessage(ChannelId channelId) : ExchangeMessage(channelId) {
    }
};

class EndOfStreamMessage : public ExchangeMessage {
  public:
    static constexpr MessageType MESSAGE_TYPE = EndOfStream;

    explicit EndOfStreamMessage(ChannelId channelId) : ExchangeMessage(channelId) {
    }
};

class ErrMessage : public ExchangeMessage {
  public:
    static constexpr MessageType MESSAGE_TYPE = ErrorMessage;

    explicit ErrMessage(ChannelId channelId, ErrorType error) : ExchangeMessage(channelId), error(error){};

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

class DataBufferMessage {
  public:
    static constexpr MessageType MESSAGE_TYPE = DataBuffer;

    explicit DataBufferMessage(uint32_t payloadSize, uint32_t numOfRecords) : payloadSize(payloadSize), numOfRecords(numOfRecords) {
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

}// namespace Messages
}// namespace Network
}// namespace NES

#endif//NES_NETWORKMESSAGE_HPP
