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

#include <memory>
#include <utility>
#include <API/Schema.hpp>
#include <Identifiers/NESStrongTypeFormat.hpp> /// NOLINT
#include <Sequencing/SequenceData.hpp>
#include <Time/Timestamp.hpp>
#include <fmt/format.h>

namespace NES
{

enum class JoinBuildSideType : uint8_t
{
    Right,
    Left
};

/// Stores the information of a window. The start, end, and the identifier
struct WindowInfo
{
    WindowInfo(const Timestamp windowStart, const Timestamp windowEnd) : windowStart(windowStart), windowEnd(windowEnd)
    {
        if (windowEnd < windowStart)
        {
            this->windowStart = Timestamp(0);
        }
    }

    WindowInfo(const uint64_t windowStart, const uint64_t windowEnd) : windowStart(windowStart), windowEnd(windowEnd)
    {
        if (windowEnd < windowStart)
        {
            this->windowStart = Timestamp(0);
        }
    }

    bool operator<(const WindowInfo& other) const { return windowEnd < other.windowEnd; }
    Timestamp windowStart;
    Timestamp windowEnd;
};

/// Stores the metadata for a RecordBuffer
struct BufferMetaData
{
    BufferMetaData(const Timestamp watermarkTs, const SequenceData seqNumber, const OriginId originId)
        : watermarkTs(watermarkTs), seqNumber(seqNumber), originId(originId)
    {
    }

    [[nodiscard]] std::string toString() const
    {
        return fmt::format("BufferMetadata(waterMarkTs: {}, seqNumber: {}, originId: {})", watermarkTs, seqNumber, originId);
    }

    Timestamp watermarkTs;
    SequenceData seqNumber;
    OriginId originId;
};


/// This stores the left, right and output schema for a binary join
struct JoinSchema
{
    JoinSchema(Schema leftSchema, Schema rightSchema, Schema joinSchema)
        : leftSchema(std::move(leftSchema)), rightSchema(std::move(rightSchema)), joinSchema(std::move(joinSchema))
    {
    }

    Schema leftSchema;
    Schema rightSchema;
    Schema joinSchema;
};
}
