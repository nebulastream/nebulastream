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

#ifndef NES_INCLUDE_NETWORK_CHANNEL_ID_HPP_
#define NES_INCLUDE_NETWORK_CHANNEL_ID_HPP_

#include <Network/NesPartition.hpp>
#include <Runtime/NesThread.hpp>

namespace NES {
namespace Network {

class ChannelId {
  public:
    explicit ChannelId(NesPartition nesPartition, uint64_t nodeEngineId) : nesPartition(nesPartition), nodeEngineId(nodeEngineId) {
        // nop
    }

    [[nodiscard]] NesPartition getNesPartition() const { return nesPartition; }

    [[nodiscard]] uint64_t getNodeEngineId() const { return nodeEngineId; }

    [[nodiscard]] std::string toString() const { return nesPartition.toString() + "(nodeEngineId=" + std::to_string(nodeEngineId) + ")"; }

    friend std::ostream& operator<<(std::ostream& os, const ChannelId& channelId) { return os << channelId.toString(); }

  private:
    const NesPartition nesPartition;
    uint64_t nodeEngineId;
};
}// namespace Network
}// namespace NES
#endif// NES_INCLUDE_NETWORK_CHANNEL_ID_HPP_
