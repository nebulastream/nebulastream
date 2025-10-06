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
#pragma once

#include <Nautilus/DataStructures/OffsetBasedHashMap.hpp>
#include <Nautilus/DataStructures/SerializablePagedVector.hpp>
#include <Nautilus/State/Reflection/StateDefinitions.hpp>

namespace NES::State {

// Hash map conversions: OffsetHashMapState <-> JoinState::HashMap
inline JoinState::HashMap toState(const DataStructures::OffsetHashMapState& s) {
    JoinState::HashMap h{};
    h.memory = s.memory;
    h.chains = s.chains;
    h.entrySize = s.entrySize;
    h.tupleCount = s.tupleCount;
    h.keySize = s.keySize;
    h.valueSize = s.valueSize;
    h.bucketCount = s.bucketCount;
    h.varSizedMemory = s.varSizedMemory;
    h.varSizedOffset = s.varSizedOffset;
    return h;
}

inline DataStructures::OffsetHashMapState fromState(const JoinState::HashMap& h) {
    DataStructures::OffsetHashMapState s{};
    s.memory = h.memory;
    s.chains = h.chains;
    s.entrySize = h.entrySize;
    s.tupleCount = h.tupleCount;
    s.keySize = h.keySize;
    s.valueSize = h.valueSize;
    s.bucketCount = h.bucketCount;
    s.varSizedMemory = h.varSizedMemory;
    s.varSizedOffset = h.varSizedOffset;
    return s;
}

// PagedVector conversions: PagedVectorState is already serializable; runtime uses SerializablePagedVector
inline JoinState::VectorState toState(const DataStructures::PagedVectorState& v) { return v; }
inline DataStructures::PagedVectorState fromState(const JoinState::VectorState& v) { return v; }

}

