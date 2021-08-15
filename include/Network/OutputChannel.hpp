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

#ifndef NES_INCLUDE_NETWORK_OUTPUT_CHANNEL_HPP_
#define NES_INCLUDE_NETWORK_OUTPUT_CHANNEL_HPP_

#include <Network/NetworkMessage.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <iostream>
#include <memory>
#include <queue>
#include <zmq.hpp>

namespace NES {
namespace Network {

class ExchangeProtocol;

/**
 * NOT THREAD SAFE! DON'T SHARE AMONG THREADS!
 *
 */
class OutputChannel;
using OutputChannelPtr = std::unique_ptr<OutputChannel>;

class OutputChannel {
  public:
    explicit OutputChannel(zmq::socket_t&& zmqSocket, ChannelId channelId, std::string&& address,std::queue<std::pair<Runtime::TupleBuffer, uint64_t>>&& buffer);

    /**
     * @brief close the output channel and release resources
     */
    ~OutputChannel() { close(); }

    OutputChannel(const OutputChannel&) = delete;

    OutputChannel& operator=(const OutputChannel&) = delete;

    /**
     * @brief Creates an output channe instance with the given parameters
     * @param zmqContext the local zmq server context
     * @param address the ip address of the remote server
     * @param nesPartition the remote nes partition to connect to
     * @param protocol the protocol implementation
     * @param waitTime the backoff time in case of failure when connecting
     * @param retryTimes the number of retries before the methods will raise error
     * @return
     */
    static OutputChannelPtr create(const std::shared_ptr<zmq::context_t>& zmqContext,
                                   std::string&& socketAddr,
                                   NesPartition nesPartition,
                                   ExchangeProtocol& protocol,
                                   std::chrono::seconds waitTime,
                                   uint8_t retryTimes,
                                   std::queue<std::pair<Runtime::TupleBuffer, uint64_t>>&& buffer = {});

    /**
     * @brief Send buffer to the destination defined in the constructor. Note that this method will internally
     * compute the payloadSize as tupleSizeInBytes*buffer.getNumberOfTuples()
     * @param the inputBuffer to send
     * @param the tupleSize represents the size in bytes of one tuple in the buffer
     * @return true if send was successful, else false
     */
    bool sendBuffer(Runtime::TupleBuffer inputBuffer, uint64_t tupleSizeInBytes);

    /**
     * @brief Method to handle the error
     * @param the error message
     */
    static void onError(Messages::ErrorMessage& errorMsg);

    /**
     * Close the outchannel and send EndOfStream message to consumer
     */
    void close();

    /**
     * shut down ZMQ sockt without sending EoS message. Used for updating networkSinkOut
     */
    void shutdownZMQSocket(bool withMessagePropagation);

    /**
     * Sends all TupleBuffers stored in buffer.
     * Sets buffering flag to false
     * @param b
     */
    bool emptyBuffer();

    void setBuffer(bool b);

    bool isBuffering();

    std::queue<std::pair<Runtime::TupleBuffer, uint64_t>>&& moveBuffer();

  private:
    const std::string socketAddr;
    zmq::socket_t zmqSocket;
    const ChannelId channelId;
    bool isClosed{false};
    std::queue<std::pair<Runtime::TupleBuffer, uint64_t>> buffer;
    bool buffering;
};

}// namespace Network
}// namespace NES

#endif// NES_INCLUDE_NETWORK_OUTPUT_CHANNEL_HPP_
