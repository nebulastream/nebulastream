#ifndef NES_ZMQUTILS_HPP
#define NES_ZMQUTILS_HPP

#include <Network/NetworkMessage.hpp>
#include <Network/NetworkCommon.hpp>
#include <zmq.hpp>

namespace NES {
namespace Network {

    template <typename MessageType, typename... Arguments>
    void sendMessage(zmq::socket_t& zmqSocket, Arguments&&... args) {
        Messages::MessageHeader header(MessageType::MESSAGE_TYPE, sizeof(MessageType));
        MessageType message(std::forward<Arguments>(args)...);
        zmq::message_t sendHeader(&header, sizeof(Messages::MessageHeader));
        zmq::message_t sendMsg(&message, sizeof(MessageType));
        zmqSocket.send(sendHeader, zmq::send_flags::sndmore);
        zmqSocket.send(sendMsg);
    }

    template <typename MessageType, typename... Arguments>
    void sendMessageWithIdentity(zmq::socket_t& zmqSocket, zmq::message_t& zmqIdentity, Arguments&&... args) {
        Messages::MessageHeader header(MessageType::MESSAGE_TYPE, sizeof(MessageType));
        MessageType message(std::forward<Arguments>(args)...);
        zmq::message_t sendHeader(&header, sizeof(Messages::MessageHeader));
        zmq::message_t sendMsg(&message, sizeof(MessageType));
        zmqSocket.send(zmqIdentity, zmq::send_flags::sndmore);
        zmqSocket.send(sendHeader, zmq::send_flags::sndmore);
        zmqSocket.send(sendMsg);
    }

}
}

#endif //NES_ZMQUTILS_HPP
