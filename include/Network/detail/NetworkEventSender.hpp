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

#ifndef NES_INCLUDE_NETWORK_DETAIL_NETWORKEVENTSENDER_HPP_
#define NES_INCLUDE_NETWORK_DETAIL_NETWORKEVENTSENDER_HPP_

#include <Network/ZmqUtils.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Events.hpp>

namespace NES::Network::detail {

/**
 * @brief Mixin to add event sending semantics to a base channel
 * @tparam BaseChannelType the type of the base channel
 */
template<typename BaseChannelType>
class NetworkEventSender : public BaseChannelType {
  public:
    static constexpr bool canSendData = false || BaseChannelType::canSendData;
    static constexpr bool canSendEvent = true;

    /**
     * @brief Forwarding ctor: it forwards the args to the base class
     * @tparam Args the arguments types
     * @param args the arguments
     */
    template<typename... Args>
    NetworkEventSender(Args&&... args) : BaseChannelType(std::forward<Args>(args)...) {}

    template<Runtime::IsNesEvent Event, typename... Arguments>
    bool sendEvent(Arguments&&... args) {
        auto optBuffer = this->bufferManager->getUnpooledBuffer(sizeof(Event));
        auto buffer = *optBuffer;
        auto* event = new (buffer.getBuffer()) Event(std::forward<Arguments>(args)...);
        buffer.setNumberOfTuples(sizeof(Event));
        return sendEvent(std::move(buffer), event->getEventType());
    }

    bool sendEvent(Runtime::TupleBuffer&& inputBuffer, Runtime::EventType eventType) {
        auto payloadSize = inputBuffer.getNumberOfTuples();
        auto* ptr = inputBuffer.getBuffer<uint8_t>();
        if (payloadSize == 0) {
            return true;
        }
        sendMessage<Messages::EventBufferMessage, kZmqSendMore>(this->zmqSocket, eventType, payloadSize);
        // We need to retain the `inputBuffer` here, because the send function operates asynchronously and we therefore
        // need to pass the responsibility of freeing the tupleBuffer instance to ZMQ's callback.
        inputBuffer.retain();
        auto const sentBytesOpt = this->zmqSocket.send(
            zmq::message_t(ptr, payloadSize, &Runtime::detail::zmqBufferRecyclingCallback, inputBuffer.getControlBlock()),
            kZmqSendDefault);
        if (sentBytesOpt.has_value()) {
            NES_TRACE("DataChannel: Sending buffer with " << inputBuffer.getNumberOfTuples() << "/" << inputBuffer.getBufferSize()
                                                          << "-" << inputBuffer.getOriginId());
            return true;
        }
        NES_ERROR("DataChannel: Error sending buffer for " << this->channelId);
        return false;
    }
};
}// namespace NES::Network::detail

#endif//NES_INCLUDE_NETWORK_DETAIL_NETWORKEVENTSENDER_HPP_
