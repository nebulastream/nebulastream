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

#ifndef NES_OUTPUTCHANNELKEY_HPP
#define NES_OUTPUTCHANNELKEY_HPP

#include <Network/NetworkMessage.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <iostream>
#include <memory>
#include <zmq.hpp>

namespace NES::Network {

class OutputChannelKey {
  public:
    OutputChannelKey(QuerySubPlanId querySubPlanId, Network::OperatorId operatorId);
    QuerySubPlanId getQuerySubPlanId() const;
    OperatorId getOperatorId() const;
    bool operator==(const OutputChannelKey& rhs) const;
    bool operator!=(const OutputChannelKey& rhs) const;
    friend std::ostream& operator<<(std::ostream& os, const OutputChannelKey& key);

  private:
    QuerySubPlanId querySubPlanId;
    Network::OperatorId operatorId;
};

}// namespace NES
namespace std {
template<>
struct hash<NES::Network::OutputChannelKey> {
    std::uint64_t operator()(const NES::Network::OutputChannelKey& k) const {
        using std::hash;

        // Hash function for the ChannelKey
        // Compute individual hash values of the Ints and combine them using XOR and bit shifting:
        return ((hash<uint64_t>()(k.getQuerySubPlanId()) ^ (hash<uint64_t>()(k.getOperatorId()) << 1)) >> 1);
    }
};

}// namespace std
#endif//NES_OUTPUTCHANNELKEY_HPP
