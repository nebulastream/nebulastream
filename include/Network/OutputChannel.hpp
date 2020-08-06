#ifndef NES_OUTPUTCHANNEL_HPP
#define NES_OUTPUTCHANNEL_HPP

#include <Network/ExchangeProtocol.hpp>
#include <Network/NetworkMessage.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <iostream>
#include <memory>
#include <zmq.hpp>

namespace NES {
namespace Network {

/**
 * NOT THREAD SAFE! DON'T SHARE AMONG THREADS!
 *
 */
class OutputChannel;
typedef std::unique_ptr<OutputChannel> OutputChannelPtr;

class OutputChannel {
  public:
    explicit OutputChannel(
        zmq::socket_t&& zmqSocket,
        const ChannelId channelId,
        const std::string address);

  public:
    /**
     * @brief close the output channel and release resources
     */
    ~OutputChannel() {
        close();
    }

    OutputChannel(const OutputChannel&) = delete;

    OutputChannel& operator=(const OutputChannel&) = delete;

  public:
    /**
     * @brief Creates an output channe instance with the given parameters
     * @param zmqContext the local zmq server context
     * @param address the ip address of the remote server
     * @param nesPartition the remote nes partition to connect to
     * @param protocol the protocol implementation
     * @param waitTime the backoff time in case of failure when connecting
     * @param retryTimes the number of retries before the methods will raise error
     * @param threadId the id of the thread accessing the channel (this will go away when #766 is in place)
     * @return
     */
    static OutputChannelPtr create(std::shared_ptr<zmq::context_t> zmqContext,
                                   const std::string address,
                                   NesPartition nesPartition,
                                   ExchangeProtocol& protocol,
                                   std::chrono::seconds waitTime,
                                   uint8_t retryTimes,
                                   size_t threadId = std::hash<std::thread::id>{}(std::this_thread::get_id()));

    /**
     * @brief Send buffer to the destination defined in the constructor. Note that this method will internally
     * compute the payloadSize as tupleSizeInBytes*buffer.getNumberOfTuples()
     * @param the inputBuffer to send
     * @param the tupleSize represents the size in bytes of one tuple in the buffer
     * @return true if send was successful, else false
     */
    bool sendBuffer(TupleBuffer& inputBuffer, size_t tupleSizeInBytes);

    /**
     * @brief Method to handle the error
     * @param the error message
     */
    void onError(Messages::ErrorMessage& errorMsg);

    /**
     * Close the outchannel and send EndOfStream message to consumer
     */
    void close();

  private:
    const std::string socketAddr;
    zmq::socket_t zmqSocket;
    const ChannelId channelId;
    bool isClosed;
};

}// namespace Network
}// namespace NES

#endif//NES_OUTPUTCHANNEL_HPP
