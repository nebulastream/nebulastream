/*
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

#ifndef NES_INCLUDE_NETWORK_NETWORKMESSAGE_HPP_
#define NES_INCLUDE_NETWORK_NETWORKMESSAGE_HPP_

#include <Network/ChannelId.hpp>
#include <Runtime/Events.hpp>
#include <cstdint>
#include <stdexcept>
#include <utility>

namespace NES {
namespace Network {
namespace Messages {

/**
 * @brief This magic number is written as first 64bits of every NES network message.
 * We use this as a checksum to validate that we are not transferring garbage data.
 */
using nes_magic_number_t = uint64_t;
static constexpr nes_magic_number_t NES_NETWORK_MAGIC_NUMBER = 0xBADC0FFEE;

enum class MessageType : uint8_t {
    /// message type that the client uses to announce itself to the server
    ClientAnnouncement,
    /// message type that the servers uses to reply to the client regarding the availability
    /// of a partition
    ServerReady,
    /// message type of a data buffer
    DataBuffer,
    /// type of a message that contains an error
    ErrorMessage,
    /// type of a message that marks a stream subpartition as finished, i.e., no more records are expected
    EndOfStream,
    /// message type of an event buffer
    EventBuffer,
};

/// this enum defines the errors that can occur in the network stack logic
enum class ErrorType : uint8_t {
    /// error raised when requesting a partition that is not registered
    PartitionNotRegisteredError,
    /// error raised when a data/event buffer arrives for a partition that is not known on the current node
    UnknownPartitionError,
    /// error raised when requesting a partition that has been previously deleted
    DeletedPartitionError,
    /// error raised when there is no known reason
    UnknownError,
};

enum class ChannelType : uint8_t {
    /// data channel: allows sending data and event buffers
    DataChannel,
    /// event-only channel: allows sending event buffers only
    EventOnlyChannel
};

/*
    This is how a NES Network Message looks like on the wire

    +------------------+-----------------+-----------------------+
    |  Zmq Routing Id  |  MessageHeader  |   OPTIONAL subclass   |
    |   NesChannelId   |    13 bytes     |   of ExchangeMessage  |
    |     8 bytes      |                 |     has var size      |
    +------------------+-----------------+-----------------------+

 */

/**
 * @brief this is the pramble of each message that is sent via the network
 */
class MessageHeader {
  public:
    explicit MessageHeader(MessageType msgType, uint32_t msgLength)
        : magicNumber(NES_NETWORK_MAGIC_NUMBER), msgType(msgType), msgLength(msgLength) {}

    [[nodiscard]] nes_magic_number_t getMagicNumber() const { return magicNumber; }

    [[nodiscard]] MessageType getMsgType() const { return msgType; }

    [[nodiscard]] uint32_t getMsgLength() const { return msgLength; }

  private:
    /// this is a magic number that we use as checksum
    const nes_magic_number_t magicNumber;
    /// type of the message that follows as payload
    const MessageType msgType;
    /// size of the payload message
    const uint32_t msgLength;
};

/**
 * @brief This is the base class for all messages that can be sent in NES
 */
class ExchangeMessage {
  public:
    explicit ExchangeMessage(ChannelId channelId) : channelId(std::move(channelId)) {}

    [[nodiscard]] const ChannelId& getChannelId() const { return channelId; }

  private:
    const ChannelId channelId;
};

/**
 * @brief This message is sent when a client announces itself to a server. It's the first message that is sent.
 */
class ClientAnnounceMessage : public ExchangeMessage {
  public:
    static constexpr MessageType MESSAGE_TYPE = MessageType::ClientAnnouncement;

    explicit ClientAnnounceMessage(ChannelId channelId, ChannelType mode) : ExchangeMessage(channelId), mode(mode) {}

    ChannelType getMode() const { return mode; }

  private:
    ChannelType mode;
};

/**
 * @brief This message is sent back to a client when a server is ready to receive data.
 */
class ServerReadyMessage : public ExchangeMessage {
  public:
    static constexpr MessageType MESSAGE_TYPE = MessageType::ServerReady;

    explicit ServerReadyMessage(ChannelId channelId) : ExchangeMessage(channelId) {
        // nop
    }
};

/**
 * @brief This message is sent to notify end-of-stream.
 */
class EndOfStreamMessage : public ExchangeMessage {
  public:
    static constexpr MessageType MESSAGE_TYPE = MessageType::EndOfStream;

    explicit EndOfStreamMessage(ChannelId channelId, ChannelType channelType, bool graceful = true)
        : ExchangeMessage(channelId), channelType(channelType), graceful(graceful) {}

    [[nodiscard]] bool isGraceful() const { return graceful; }

    [[nodiscard]] bool isDataChannel() const { return channelType == ChannelType::DataChannel; }

    [[nodiscard]] bool isEventChannel() const { return channelType == ChannelType::EventOnlyChannel; }

  private:
    ChannelType channelType;
    bool graceful;
};

/**
 * @brief This message represent an error that is sent from the client to the server or vice versa.
 */
class ErrorMessage : public ExchangeMessage {
  public:
    static constexpr MessageType MESSAGE_TYPE = MessageType::ErrorMessage;

    explicit ErrorMessage(ChannelId channelId, ErrorType error) : ExchangeMessage(channelId), errorCode(error) {
        // nop
    }

    [[nodiscard]] ErrorType getErrorType() const { return errorCode; }

    [[nodiscard]] std::string getErrorTypeAsString() const {
        if (errorCode == ErrorType::PartitionNotRegisteredError) {
            return "PartitionNotRegisteredError";
        } else if (errorCode == ErrorType::DeletedPartitionError) {
            return "DeletedPartitionError";
        }
        return "UnknownError";
    }

    /**
     * @brief this checks if the message contains a PartitionNotRegisteredError
     * @return true if the message contains a PartitionNotRegisteredError
     */
    [[nodiscard]] bool isPartitionNotFound() const { return errorCode == ErrorType::PartitionNotRegisteredError; }

    /**
     * @brief this checks if the message contains a DeletedPartitionError
     * @return true if the message contains a DeletedPartitionError
     */
    [[nodiscard]] bool isPartitionDeleted() const { return errorCode == ErrorType::DeletedPartitionError; }

  private:
    const ErrorType errorCode;
};

/**
 * @brief This is the payload with tuples
 */
class DataBufferMessage {
  public:
    static constexpr MessageType MESSAGE_TYPE = MessageType::DataBuffer;

    explicit inline DataBufferMessage(uint32_t payloadSize,
                                      uint32_t numOfRecords,
                                      uint64_t originId,
                                      uint64_t watermark,
                                      uint64_t creationTimestamp,
                                      uint64_t sequenceNumber) noexcept
        : payloadSize(payloadSize), numOfRecords(numOfRecords), originId(originId), watermark(watermark),
          creationTimestamp(creationTimestamp), sequenceNumber(sequenceNumber) {}

    uint32_t const payloadSize;
    uint32_t const numOfRecords;
    uint64_t const originId;
    uint64_t const watermark;
    uint64_t const creationTimestamp;
    uint64_t const sequenceNumber;
};

/**
 * @brief This a payload message with an event
 */
class EventBufferMessage {
  public:
    static constexpr MessageType MESSAGE_TYPE = MessageType::EventBuffer;

    explicit inline EventBufferMessage(Runtime::EventType eventType, uint32_t payloadSize) noexcept
        : eventType(eventType), payloadSize(payloadSize) {}

    Runtime::EventType const eventType;
    uint32_t const payloadSize;
};

}// namespace Messages
}// namespace Network
}// namespace NES

#endif  // NES_INCLUDE_NETWORK_NETWORKMESSAGE_HPP_
