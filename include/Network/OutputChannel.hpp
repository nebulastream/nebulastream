#ifndef NES_OUTPUTCHANNEL_HPP
#define NES_OUTPUTCHANNEL_HPP

#include <Network/ExchangeProtocol.hpp>
#include <Network/NetworkMessage.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <boost/core/noncopyable.hpp>
#include <iostream>
#include <memory>
#include <zmq.hpp>

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
        const std::string& address,
        NesPartition nesPartition,
        std::chrono::seconds waitTime, uint8_t retryTimes,
        std::function<void(Messages::ErrMessage)> onError,
        size_t threadId = std::hash<std::thread::id>{}(std::this_thread::get_id()));

    ~OutputChannel() {
        close();
    }

  private:
    void init(std::chrono::seconds waitTime, uint8_t retryTimes);
    bool registerAtServer();

  public:
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
    void onError(Messages::ErrMessage& errorMsg);

    /**
     * Close the outchannel and send EndOfStream message to consumer
     */
    void close();

    /**
     * @brief checks if the OutputChannel has successfully established a connection with the server
     * @return true if connection is established, else false
     */
    bool isConnected() const;

  private:
    const std::string& socketAddr;

    zmq::socket_t zmqSocket;
    const ChannelId channelId;

    bool isClosed;
    bool connected;

    std::function<void(Messages::ErrMessage)> onErrorCb;
};

}// namespace Network
}// namespace NES

#endif//NES_OUTPUTCHANNEL_HPP
