#ifndef NES_OUTPUTCHANNEL_HPP
#define NES_OUTPUTCHANNEL_HPP

#include <Network/NetworkMessage.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Network/ExchangeProtocol.hpp>
#include <boost/core/noncopyable.hpp>
#include <iostream>
#include <memory>

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
        uint8_t waitTime, uint8_t retryTimes,
        std::function<void(Messages::ErroMessage)> onError);

    ~OutputChannel() {
        close();
    }

  private:
    void init(u_int64_t waitTime, u_int64_t retryTimes);
    bool registerAtServer();

  public:
    /**
     * @brief Send buffer to the destination zmqContext defined in the constructor.
     * @param the inputBuffer
     * @return true if send was successfull, else false
     */
    bool sendBuffer(TupleBuffer& inputBuffer);

    /**
     * @brief Method to handle the error
     * @param the error message
     */
    void onError(Messages::ErroMessage& errorMsg);

    /**
     * Close the outchannel and send EndOfStream message to consumer
     */
    void close();

    /**
     * @brief checks if the OutputChannel has successfully established a connection with the server
     * @return true if connection is established, else false
     */
    bool isConnected();

  private:
    const std::string& socketAddr;

    zmq::socket_t zmqSocket;
    const NesPartition nesPartition;

    bool isClosed;
    bool connected;

    std::function<void(Messages::ErroMessage)> onErrorCb;
};

}// namespace Network
}// namespace NES

#endif//NES_OUTPUTCHANNEL_HPP
