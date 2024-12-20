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
#include <cstdint>
#include <map>
#include <ostream>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <fmt/base.h>
#include <fmt/ostream.h>
#include <folly/Synchronized.h>

namespace NES::Runtime::Execution::Operators
{

/**
 * @brief Stores a sequenceNumber and an OriginId
 */
struct SeqNumberOriginId
{
    SequenceNumber sequenceNumber = INVALID_SEQ_NUMBER;
    OriginId originId = INVALID_ORIGIN_ID;

    auto operator<=>(const SeqNumberOriginId&) const = default;
    friend std::ostream& operator<<(std::ostream& os, const SeqNumberOriginId& obj)
    {
        return os << "{ seqNumber = " << obj.sequenceNumber << ", originId = " << obj.originId << "}";
    }
};

/**
 * @brief Container for storing information, related to the state of a sequence number
 */
struct SequenceState
{
    uint64_t lastChunkNumber = INVALID_CHUNK_NUMBER.getRawValue();
    uint64_t seenChunks = 0;
};

class EmitOperatorHandler final : public OperatorHandler
{
public:
    EmitOperatorHandler() = default;

    /**
    * @brief Returns the next chunk number belonging to a sequence number for emitting a buffer
    * @param seqNumberOriginId
    * @return uint64_t
    */
    uint64_t getNextChunkNumber(SeqNumberOriginId seqNumberOriginId);

    /**
     * @brief Checks if this PipelineExecutionContext has seen all chunks for a given sequence number.
     * @param seqNumberOriginId
     * @param chunkNumber
     * @param isLastChunk
     * @return True, if all chunks have been seen, false otherwise
     */
    bool isLastChunk(SeqNumberOriginId seqNumberOriginId, ChunkNumber chunkNumber, bool isLastChunk);

    /**
     * @brief Removes the sequence state in seqNumberOriginIdToChunkStateInput and seqNumberOriginIdToOutputChunkNumber
     * for the seqNumberOriginId
     * @param seqNumberOriginId
     */
    void removeSequenceState(SeqNumberOriginId seqNumberOriginId);

    void start(PipelineExecutionContext& pipelineExecutionContext, uint32_t localStateVariableId) override;
    void stop(Runtime::QueryTerminationType terminationType, PipelineExecutionContext& pipelineExecutionContext) override;

    folly::Synchronized<std::map<SeqNumberOriginId, SequenceState>> seqNumberOriginIdToChunkStateInput;
    folly::Synchronized<std::map<SeqNumberOriginId, ChunkNumber::Underlying>> seqNumberOriginIdToOutputChunkNumber;
};
}

namespace fmt
{
template <>
struct fmt::formatter<NES::Runtime::Execution::Operators::SeqNumberOriginId> : ostream_formatter
{
};
}
