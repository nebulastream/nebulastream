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
#include <chrono>
#include <cstddef>
#include <exception>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/ThreadNaming.hpp>
#include <fmt/format.h>
#include <AsyncSourceImpl.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
void addBufferMetaData(OriginId originId, SequenceNumber sequenceNumber, TupleBuffer& buffer)
{
    /// set the origin id for this source
    buffer.setOriginId(originId);
    /// set the creation timestamp
    buffer.setCreationTimestampInMS(Timestamp(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count()));
    /// Set the sequence number of this buffer.
    /// A data source generates a monotonic increasing sequence number
    buffer.setSequenceNumber(sequenceNumber);
    buffer.setChunkNumber(INITIAL_CHUNK_NUMBER);
    buffer.setLastChunk(true);
    NES_TRACE(
        "Setting the buffer metadata for source {} with originId={} sequenceNumber={} chunkNumber={} lastChunk={}",
        buffer.getOriginId(),
        buffer.getOriginId(),
        buffer.getSequenceNumber(),
        buffer.getChunkNumber(),
        buffer.isLastChunk());
}
}

bool AsyncSourceImpl::start(SourceReturnType::EmitFunction&& emitFunction)
{
    sourceImplementation->open(
        bufferManager,
        AsyncSource::AsyncSourceEmit(
            [emitFunction = std::move(emitFunction), seq = SequenceNumber::INITIAL, oid = originId](
                SourceReturnType::SourceReturnType rt) mutable
            {
                if (auto* data = std::get_if<SourceReturnType::Data>(&rt))
                {
                    addBufferMetaData(oid, SequenceNumber(seq++), data->buffer);
                }
                return emitFunction(oid, rt, {});
            }));
    return true;
}

std::ostream& AsyncSourceImpl::toString(std::ostream& out) const
{
    out << "\nAsyncSourceImpl(";
    out << "\n  originId: " << originId;
    out << "\n  source implementation:" << *sourceImplementation;
    out << ")\n";
    return out;
}

std::ostream& operator<<(std::ostream& out, const AsyncSourceImpl& sourceThread)
{
    return sourceThread.toString(out);
}

}
