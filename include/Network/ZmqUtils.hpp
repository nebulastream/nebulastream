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

#ifndef NES_ZMQUTILS_HPP
#define NES_ZMQUTILS_HPP

#include <Network/NesPartition.hpp>
#include <Network/NetworkMessage.hpp>
#include <zmq.hpp>

namespace NES {
namespace Network {

#if ZMQ_VERSION_MAJOR >= 4 && ZMQ_VERSION_MINOR >= 3 && ZMQ_VERSION_PATCH >= 3
static constexpr zmq::send_flags kSendMore = zmq::send_flags::sndmore;
#else
static constexpr int kSendMore = ZMQ_SNDMORE;
#endif

/**
     * Send a message MessageType(args) through an open zmqSocket
     * @tparam MessageType
     * @tparam Arguments
     * @param zmqSocket
     * @param args
     */
template<typename MessageType, int flags = 0, typename... Arguments>
void sendMessage(zmq::socket_t& zmqSocket, Arguments&&... args) {
    // create a header message for MessageType
    Messages::MessageHeader header(MessageType::MESSAGE_TYPE, sizeof(MessageType));
    // create a payload MessageType object to send via zmq
    MessageType message(std::forward<Arguments>(args)...);// perfect forwarding
    // create zmq envelopes
    zmq::message_t sendHeader(&header, sizeof(Messages::MessageHeader));
    zmq::message_t sendMsg(&message, sizeof(MessageType));
    // send both messages in one shot
    zmqSocket.send(sendHeader, kSendMore);
    zmqSocket.send(sendMsg, flags);
}

/**
     * Send a zmqIdentity followed by a message MessageType(args) via zmqSocket
     * @tparam MessageType
     * @tparam Arguments
     * @param zmqSocket
     * @param args
     */
template<typename MessageType, typename... Arguments>
void sendMessageWithIdentity(zmq::socket_t& zmqSocket, zmq::message_t& zmqIdentity, Arguments&&... args) {
    // create a header message for MessageType
    Messages::MessageHeader header(MessageType::MESSAGE_TYPE, sizeof(MessageType));
    // create a payload MessageType object using args
    MessageType message(std::forward<Arguments>(args)...);// perfect forwarding
    // create zmq envelopes
    zmq::message_t sendHeader(&header, sizeof(Messages::MessageHeader));
    zmq::message_t sendMsg(&message, sizeof(MessageType));
    // send all messages in one shot
    zmqSocket.send(zmqIdentity, kSendMore);
    zmqSocket.send(sendHeader, kSendMore);
    zmqSocket.send(sendMsg);
}

}// namespace Network
}// namespace NES

#endif//NES_ZMQUTILS_HPP
