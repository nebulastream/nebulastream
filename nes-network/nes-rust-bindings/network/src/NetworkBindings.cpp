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

#include <NetworkBindings.hpp>

#include <algorithm>
#include <cstdint>
#include <string>

#include <Identifiers/Identifiers.hpp>
#include <Time/Timestamp.hpp>
#include <network/lib.h>
#include <rust/cxx.h>
#include <ErrorHandling.hpp>
#include <NetworkOptions.hpp>
#include <Thread.hpp>

void initNetworkServices( /// NOLINT(misc-use-internal-linkage)
    const std::string& connectionAddr,
    const NES::WorkerId& workerId,
    const NES::NetworkOptions& options)
{
    const NetworkServiceOptions cxxOptions{
        .sender_queue_size = options.senderQueueSize,
        .max_pending_acks = options.maxPendingAcks,
        .receiver_queue_size = options.receiverQueueSize,
        .sender_io_threads = options.senderIOThreads,
        .receiver_io_threads = options.receiverIOThreads,
    };
    init_receiver_service(rust::String(connectionAddr), rust::String(workerId.getRawValue()), cxxOptions);
    init_sender_service(rust::String(connectionAddr), rust::String(workerId.getRawValue()), cxxOptions);
}

void TupleBufferBuilder::setMetadata(const SerializedTupleBufferHeader& metaData)
{
    buffer.setSequenceNumber(NES::SequenceNumber(metaData.sequence_number));
    buffer.setChunkNumber(NES::ChunkNumber(metaData.chunk_number));
    buffer.setOriginId(NES::OriginId(metaData.origin_id));
    buffer.setLastChunk(metaData.last_chunk);
    buffer.setWatermark(NES::Timestamp(metaData.watermark));
    buffer.setNumberOfTuples(metaData.number_of_tuples);
}

void TupleBufferBuilder::setData(rust::Slice<const uint8_t> data)
{
    INVARIANT(
        buffer.getBufferSize() >= data.length(),
        "Buffer size mismatch. Internal BufferSize: {} vs. External {}",
        buffer.getBufferSize(),
        data.length());

    std::ranges::copy(data, buffer.getAvailableMemoryArea<uint8_t>().begin());
}

void TupleBufferBuilder::addChildBuffer(const rust::Slice<const uint8_t> child)
{
    auto childBuffer = bufferProvider.getUnpooledBuffer(child.size());
    if (!childBuffer)
    {
        throw NES::CannotAllocateBuffer("allocating child buffer");
    }

    INVARIANT(
        childBuffer->getBufferSize() >= child.length(),
        "Unpooled Buffer size mismatch. Internal BufferSize: {} vs. External {}",
        childBuffer->getBufferSize(),
        child.length());

    std::ranges::copy(child, childBuffer->getAvailableMemoryArea<uint8_t>().begin());
    [[maybe_unused]] auto childIndex
        = buffer.storeChildBuffer(*childBuffer); /// index should already be present in the owning parent buffer
}

void identifyThread(const rust::str threadName, const rust::str workerId) /// NOLINT(misc-use-internal-linkage)
{
    NES::Thread::ThreadName = static_cast<std::string>(threadName);
    NES::Thread::WorkerNodeId = NES::WorkerId(static_cast<std::string>(workerId));
}
