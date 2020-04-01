#ifndef NES_ZMQUTILS_HPP
#define NES_ZMQUTILS_HPP

#include <zmq.hpp>
#include <Network/NetworkMessage.hpp>
#include <Network/NetworkCommon.hpp>


namespace NES {
namespace Network {

#if ZMQ_VERSION_MAJOR >= 4 && ZMQ_VERSION_MINOR >= 3 && ZMQ_VERSION_PATCH >= 3
    static constexpr zmq::send_flags kSendMore = zmq::send_flags::sndmore;
#else
    static constexpr int kSendMore = ZMQ_SNDMORE;
#endif

    /**
     * Send a message MessageType(args) via zmqSocket
     * @tparam MessageType
     * @tparam Arguments
     * @param zmqSocket
     * @param args
     */
    template <typename MessageType, typename... Arguments>
    void sendMessage(zmq::socket_t& zmqSocket, Arguments&&... args) {
        Messages::MessageHeader header(MessageType::MESSAGE_TYPE, sizeof(MessageType));
        MessageType message(std::forward<Arguments>(args)...);
        zmq::message_t sendHeader(&header, sizeof(Messages::MessageHeader));
        zmq::message_t sendMsg(&message, sizeof(MessageType));
        zmqSocket.send(sendHeader, kSendMore);
        zmqSocket.send(sendMsg);
    }

    /**
     * Send a zmqIdentity followed by the message MessageType(args) via zmqSocket
     * @tparam MessageType
     * @tparam Arguments
     * @param zmqSocket
     * @param args
     */
    template <typename MessageType, typename... Arguments>
    void sendMessageWithIdentity(zmq::socket_t& zmqSocket, zmq::message_t& zmqIdentity, Arguments&&... args) {
        Messages::MessageHeader header(MessageType::MESSAGE_TYPE, sizeof(MessageType));
        MessageType message(std::forward<Arguments>(args)...);
        zmq::message_t sendHeader(&header, sizeof(Messages::MessageHeader));
        zmq::message_t sendMsg(&message, sizeof(MessageType));
        zmqSocket.send(zmqIdentity, kSendMore);
        zmqSocket.send(sendHeader, kSendMore);
        zmqSocket.send(sendMsg);
    }

}
}

#endif //NES_ZMQUTILS_HPP
