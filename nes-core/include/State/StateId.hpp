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

#ifndef NES_INCLUDE_STATE_STATEID_HPP_
#define NES_INCLUDE_STATE_STATEID_HPP_

#include <cstdint>
#include <functional>

namespace NES {
/**
     * This structure represents a key in a hashmap of state variables, which is stored in the StateManager.
     * The key defines a state variable in unique way consisting of a node id, handler id and a local state variable id
     * for the case when an operator has more than one state variable.
     */
struct StateId {
    uint64_t nodeId;
    uint64_t handlerId;
    uint32_t localId;

    friend bool operator==(const StateId& left, const StateId& right) {
        return (left.nodeId == right.nodeId && left.handlerId == right.handlerId && left.localId == right.localId);
    }
};
}// namespace NES
namespace std {
template<>
struct hash<NES::StateId> {
    uint64_t operator()(const NES::StateId& stateId) const {
        typedef unsigned long ulong;
        return ulong(stateId.nodeId << 16) | ulong(stateId.handlerId << 8) | ulong(stateId.localId);
    }
};
}// namespace std

#endif  // NES_INCLUDE_STATE_STATEID_HPP_
