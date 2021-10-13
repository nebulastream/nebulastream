/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_NETWORK_NETWORK_MESSAGE_HPP_
#define NES_INCLUDE_NETWORK_NETWORK_MESSAGE_HPP_

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
    kClientAnnouncement,
    /// message type that the servers uses to reply to the client regarding the availability
    /// of a partition
    kServerReady,
    /// message type of a data buffer
    kDataBuffer,
    /// type of a message that contains an error
    kErrorMessage,
    /// type of a message that marks a stream subpartition as finished, i.e., no more records are expected
    kEndOfStream,
    /// message type of an event buffer
    kEventBuffer,
};

/// this enum defines the errors that can occur in the network stack logic
enum class ErrorType : uint8_t {
    kPartitionNotRegisteredError,
    kUnknownError,
    kUnknownPartitionError,
    kDeletedPartitionError
};

enum class ChannelType : uint8_t { kDataChannel, kEventOnlyChannel };

class MessageHeader {
  public:
    explicit MessageHeader(MessageType msgType, uint32_t msgLength)
        : magicNumber(NES_NETWORK_MAGIC_NUMBER), msgType(msgType), msgLength(msgLength) {}

    [[nodiscard]] nes_magic_number_t getMagicNumber() const { return magicNumber; }

    [[nodiscard]] MessageType getMsgType() const { return msgType; }

    [[nodiscard]] uint32_t getMsgLength() const { return msgLength; }

  private:
    const nes_magic_number_t magicNumber;
    const MessageType msgType;
    const uint32_t msgLength;
};

class ExchangeMessage {
  public:
    explicit ExchangeMessage(ChannelId channelId) : channelId(std::move(channelId)) {}

    [[nodiscard]] const ChannelId& getChannelId() const { return channelId; }

  private:
    const ChannelId channelId;
};

class ClientAnnounceMessage : public ExchangeMessage {
  public:
    static constexpr MessageType MESSAGE_TYPE = MessageType::kClientAnnouncement;

    explicit ClientAnnounceMessage(ChannelId channelId, ChannelType mode) : ExchangeMessage(channelId), mode(mode) {}

    ChannelType getMode() const { return mode; }

  private:
    ChannelType mode;
};

class ServerReadyMessage : public ExchangeMessage {
  public:
    static constexpr MessageType MESSAGE_TYPE = MessageType::kServerReady;

    explicit ServerReadyMessage(ChannelId channelId) : ExchangeMessage(channelId) {
        // nop
    }
};

class EndOfStreamMessage : public ExchangeMessage {
  public:
    static constexpr MessageType MESSAGE_TYPE = MessageType::kEndOfStream;

    explicit EndOfStreamMessage(ChannelId channelId, ChannelType channelType, bool graceful = true)
        : ExchangeMessage(channelId), channelType(channelType), graceful(graceful) {}

    [[nodiscard]] bool isGraceful() const { return graceful; }

    [[nodiscard]] bool isDataChannel() const { return channelType == ChannelType::kDataChannel; }

    [[nodiscard]] bool isEventChannel() const { return channelType == ChannelType::kEventOnlyChannel; }

  private:
    ChannelType channelType;
    bool graceful;
};

class ErrorMessage : public ExchangeMessage {
  public:
    static constexpr MessageType MESSAGE_TYPE = MessageType::kErrorMessage;

    explicit ErrorMessage(ChannelId channelId, ErrorType error) : ExchangeMessage(channelId), errorCode(error) {
        // nop
    }

    [[nodiscard]] ErrorType getErrorType() const { return errorCode; }

    [[nodiscard]] std::string getErrorTypeAsString() const {
        if (errorCode == ErrorType::kPartitionNotRegisteredError) {
            return "PartitionNotRegisteredError";
        } else if (errorCode == ErrorType::kDeletedPartitionError) {
            return "DeletedPartitionError";
        }
        return "UnknownError";
    }

    /**
     * @brief this checks if the message contains a PartitionNotRegisteredError
     * @return true if the message contains a PartitionNotRegisteredError
     */
    [[nodiscard]] bool isPartitionNotFound() const { return errorCode == ErrorType::kPartitionNotRegisteredError; }

    /**
     * @brief this checks if the message contains a DeletedPartitionError
     * @return true if the message contains a DeletedPartitionError
     */
    [[nodiscard]] bool isPartitionDeleted() const { return errorCode == ErrorType::kDeletedPartitionError; }

  private:
    const ErrorType errorCode;
};

class DataBufferMessage {
  public:
    static constexpr MessageType MESSAGE_TYPE = MessageType::kDataBuffer;

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

class EventBufferMessage {
  public:
    static constexpr MessageType MESSAGE_TYPE = MessageType::kEventBuffer;

    explicit inline EventBufferMessage(Runtime::EventType eventType, uint32_t payloadSize) noexcept
        : eventType(eventType), payloadSize(payloadSize) {}

    Runtime::EventType const eventType;
    uint32_t const payloadSize;
};

class NesNetworkError : public std::runtime_error {
  public:
    explicit NesNetworkError(ErrorMessage& msg) : std::runtime_error(msg.getErrorTypeAsString()), msg(msg) {}

    [[nodiscard]] const ErrorMessage& getErrorMessage() const { return msg; }

  private:
    const ErrorMessage msg;
};

}// namespace Messages
}// namespace Network
}// namespace NES

#endif// NES_INCLUDE_NETWORK_NETWORK_MESSAGE_HPP_
