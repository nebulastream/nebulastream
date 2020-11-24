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

#ifndef NES_INCLUDE_NETWORK_CHANNELID_HPP_
#define NES_INCLUDE_NETWORK_CHANNELID_HPP_

#include <Network/NesPartition.hpp>
#include <NodeEngine/NesThread.hpp>

namespace NES {
namespace Network {

class ChannelId {
  public:
    explicit ChannelId(NesPartition nesPartition, uint32_t threadId) : nesPartition(nesPartition), threadId(threadId) {
        // nop
    }

    NesPartition getNesPartition() const { return nesPartition; }

    uint64_t getThreadId() const { return threadId; }

    std::string toString() const { return nesPartition.toString() + "(threadId=" + std::to_string(threadId) + ")"; }

    friend std::ostream& operator<<(std::ostream& os, const ChannelId& channelId) { return os << channelId.toString(); }

  private:
    const NesPartition nesPartition;
    const uint32_t threadId;
};
}// namespace Network
}// namespace NES
#endif//NES_INCLUDE_NETWORK_CHANNELID_HPP_
