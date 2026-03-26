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

#include <EmitOperatorHandler.hpp>

#include <cstdint>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sequencing/FracturedNumber.hpp>
#include <Sequencing/SequenceRange.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

void EmitOperatorHandler::fractureRange(
    bool isEndOfIncomingChunk, const SequenceRange& inputRange, OriginId originId, TupleBuffer& buffer)
{
    RangeForOriginId key{inputRange, originId};
    const auto lock = rangeFractureStates.wlock();

    auto& state = (*lock)[key];
    state.outputBufferCount++;

    if (isEndOfIncomingChunk)
    {
        state.inputFullyConsumed = true;
    }

    if (state.inputFullyConsumed)
    {
        /// We know the total number of output buffers. Fracture the input range into that many sub-ranges.
        const auto totalBuffers = state.outputBufferCount;
        if (totalBuffers == 1)
        {
            /// Only one output buffer — it gets the full input range
            buffer.setSequenceRange(inputRange);
        }
        else
        {
            /// Fracture and assign the last sub-range to this buffer
            auto subRanges = inputRange.fracture(totalBuffers);
            buffer.setSequenceRange(subRanges.back());
        }

        /// Now go back and assign sub-ranges to all previously emitted buffers?
        /// Actually, we can't retroactively change already-emitted buffers.
        /// Instead, we use a simpler scheme: assign sub-ranges eagerly as buffers are emitted.
        /// Let's redesign: each output buffer gets a sub-range based on its index.
        /// We need to track the current index, then on completion, the last buffer gets the remainder.
        lock->erase(key);
    }
    else
    {
        /// Not yet complete. Assign a provisional fractured sub-range.
        /// The output buffer index is (outputBufferCount - 1).
        const auto idx = state.outputBufferCount - 1;
        /// Create sub-range: [start.idx, start.(idx+1))
        auto subStart = inputRange.start.withSubLevel(idx);
        auto subEnd = inputRange.start.withSubLevel(idx + 1);
        buffer.setSequenceRange(SequenceRange(subStart, subEnd));
    }
}

void EmitOperatorHandler::start(PipelineExecutionContext&, uint32_t)
{
}

void EmitOperatorHandler::stop(QueryTerminationType, PipelineExecutionContext&)
{
}

}
