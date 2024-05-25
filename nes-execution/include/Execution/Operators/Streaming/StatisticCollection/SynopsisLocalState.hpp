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

#ifndef NES_NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATISTICCOLLECTION_SYNOPSISLOCALSTATE_HPP_
#define NES_NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATISTICCOLLECTION_SYNOPSISLOCALSTATE_HPP_
#include <Execution/Operators/OperatorState.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>

namespace NES::Runtime::Execution::Operators {
struct SynopsisLocalState : public Operators::OperatorState {
    SynopsisLocalState(const Nautilus::Value<Nautilus::UInt64>& synopsisStartTs,
                       const Nautilus::Value<Nautilus::UInt64>& synopsisEndTs,
                       const Nautilus::Value<Nautilus::MemRef>& synopsisReference);
    /**
     * @brief Checks if the timestamp (ts) corresponds to the synopsis that is currently stored in the local state.
     * @param ts
     * @return True if the timestamp corresponds to the synopsis that is currently stored in the local state.
     */
    Nautilus::Value<Nautilus::Boolean> correctSynopsesForTimestamp(const Nautilus::Value<Nautilus::UInt64>& ts) const;

    Nautilus::Value<Nautilus::UInt64> synopsisStartTs;
    Nautilus::Value<Nautilus::UInt64> synopsisEndTs;
    Nautilus::Value<Nautilus::MemRef> synopsisReference;
    Nautilus::Value<Nautilus::UInt64> currentIncrement;
};
} // namespace NES::Runtime::Execution::Operators

#endif//NES_NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_STATISTICCOLLECTION_SYNOPSISLOCALSTATE_HPP_
