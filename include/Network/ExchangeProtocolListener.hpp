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

#ifndef NES_INCLUDE_NETWORK_EXCHANGEPROTOCOLLISTENER_HPP_
#define NES_INCLUDE_NETWORK_EXCHANGEPROTOCOLLISTENER_HPP_

#include <Network/NesPartition.hpp>
#include <Network/NetworkMessage.hpp>
#include <NodeEngine/TupleBuffer.hpp>

namespace NES {
namespace Network {
/**
 * @brief Listener for network stack events
 */
class ExchangeProtocolListener {
  public:
    /**
     * @brief This is called on every data buffer that the network stack receives
     * for a specific nes partition
     */
    virtual void onDataBuffer(NesPartition, NodeEngine::TupleBuffer&) = 0;
    /**
     * @brief this is called once a nes partition receives an end of stream message
     */
    virtual void onEndOfStream(Messages::EndOfStreamMessage) = 0;
    /**
     * @brief this is called on the server side as soon as an error is raised
     */
    virtual void onServerError(Messages::ErrorMessage) = 0;
    /**
     * @brief This is called on the channel side as soon as an error is raised
     */
    virtual void onChannelError(Messages::ErrorMessage) = 0;
};
}// namespace Network
}// namespace NES
#endif//NES_INCLUDE_NETWORK_EXCHANGEPROTOCOLLISTENER_HPP_
