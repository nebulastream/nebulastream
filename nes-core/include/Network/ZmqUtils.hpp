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

#ifndef NES_INCLUDE_NETWORK_ZMQUTILS_HPP_
#define NES_INCLUDE_NETWORK_ZMQUTILS_HPP_

#include <Network/NesPartition.hpp>
#include <Network/NetworkMessage.hpp>
#include <zmq.hpp>

namespace NES {
namespace Network {

#if CPPZMQ_VERSION_MAJOR >= 4 && CPPZMQ_VERSION_MINOR >= 3
static constexpr zmq::send_flags kZmqSendMore = zmq::send_flags::sndmore;
static constexpr zmq::send_flags kZmqSendDefault = zmq::send_flags::none;
static constexpr zmq::recv_flags kZmqRecvDefault = zmq::recv_flags::none;
#else
static constexpr int kZmqSendMore = ZMQ_SNDMORE;
static constexpr int kZmqSendDefault = 0;
static constexpr int KZmqRecvDefault = 0;
#endif

/**
     * Send a message MessageType(args) through an open zmqSocket
     * @tparam MessageType
     * @tparam Arguments
     * @param zmqSocket
     * @param args
     */
template<typename MessageType, decltype(kZmqSendDefault) flags = kZmqSendDefault, typename... Arguments>
void sendMessage(zmq::socket_t& zmqSocket, Arguments&&... args) {
    // create a header message for MessageType
    Messages::MessageHeader header(MessageType::MESSAGE_TYPE, sizeof(MessageType));
    // create a payload MessageType object to send via zmq
    MessageType message(std::forward<Arguments>(args)...);// perfect forwarding
    // create zmq envelopes
    zmq::message_t sendHeader(&header, sizeof(Messages::MessageHeader));
    zmq::message_t sendMsg(&message, sizeof(MessageType));
    // send both messages in one shot
    auto ret = zmqSocket.send(sendHeader, kZmqSendMore);
    NES_ASSERT2_FMT(ret.has_value(), "send failed");
    ret = zmqSocket.send(sendMsg, flags);
    NES_ASSERT2_FMT(ret.has_value(), "send failed");
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
    auto ret = zmqSocket.send(zmqIdentity, kZmqSendMore);
    NES_ASSERT2_FMT(ret.has_value(), "send failed");
    ret = zmqSocket.send(sendHeader, kZmqSendMore);
    NES_ASSERT2_FMT(ret.has_value(), "send failed");
    ret = zmqSocket.send(sendMsg, kZmqSendDefault);
    NES_ASSERT2_FMT(ret.has_value(), "send failed");
}

}// namespace Network
}// namespace NES

#endif  // NES_INCLUDE_NETWORK_ZMQUTILS_HPP_
