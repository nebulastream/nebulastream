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

#include <chrono>
#include <functional>
#include <variant>

#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES::Sources
{

using IOBuffer = Memory::TupleBuffer;

struct Data
{
    IOBuffer buffer;
};

struct EoS
{
};

struct Error
{
    Exception ex;
};

using SourceReturnType = std::variant<Error, Data, EoS>;
using EmitFunction = std::function<void(OriginId, SourceReturnType)>;

inline void addBufferMetadata(const OriginId originId, IOBuffer& buffer, const uint64_t sequenceNumber)
{
    buffer.setOriginId(originId);
    buffer.setCreationTimestampInMS(
        Runtime::Timestamp(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count()));
    buffer.setSequenceNumber(SequenceNumber{sequenceNumber});
    buffer.setChunkNumber(ChunkNumber{1});
    buffer.setLastChunk(true);

    NES_TRACE(
        "Setting buffer metadata with originId={} sequenceNumber={} chunkNumber={} lastChunk={}",
        buffer.getOriginId(),
        buffer.getSequenceNumber(),
        buffer.getChunkNumber(),
        buffer.isLastChunk());
}
}
