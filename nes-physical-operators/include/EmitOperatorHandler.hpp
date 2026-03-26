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
#include <cstddef>
#include <cstdint>
#include <map>
#include <ostream>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sequencing/SequenceRange.hpp>
#include <Util/Logger/Formatter.hpp>
#include <folly/Synchronized.h>

namespace NES
{

/// Key for tracking fracture state per (SequenceRange, OriginId)
struct RangeForOriginId
{
    SequenceRange range;
    OriginId originId = INVALID_ORIGIN_ID;

    std::strong_ordering operator<=>(const RangeForOriginId&) const = default;

    friend std::ostream& operator<<(std::ostream& os, const RangeForOriginId& obj)
    {
        return os << "{ range = " << obj.range << ", originId = " << obj.originId << "}";
    }
};

/// State for tracking how many output buffers have been produced for a given input range
struct RangeFractureState
{
    size_t outputBufferCount = 0;
    bool inputFullyConsumed = false;
};

class EmitOperatorHandler final : public OperatorHandler
{
public:
    /// Assigns a fractured sub-range to an output buffer based on the input buffer's SequenceRange.
    /// @param isEndOfIncomingChunk true if this output buffer finishes processing the current input chunk
    /// @param inputRange the SequenceRange from the input buffer
    /// @param originId the origin of the input buffer
    /// @param buffer the output buffer to assign a range to
    void fractureRange(bool isEndOfIncomingChunk, const SequenceRange& inputRange, OriginId originId, TupleBuffer& buffer);

    void start(PipelineExecutionContext& pipelineExecutionContext, uint32_t localStateVariableId) override;
    void stop(QueryTerminationType terminationType, PipelineExecutionContext& pipelineExecutionContext) override;

    folly::Synchronized<std::map<RangeForOriginId, RangeFractureState>> rangeFractureStates;
};
}

FMT_OSTREAM(NES::RangeForOriginId);
