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

#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>

#include <Identifiers/Identifiers.hpp>
#include <Time/Timestamp.hpp>
#include <network/lib.h>
#include <rust/cxx.h>
#include <ErrorHandling.hpp>


void init_receiver_service(std::string bindAddr, std::string connectionAddr)
{
    init_receiver_service(rust::String(std::move(bindAddr)), rust::String(std::move(connectionAddr)));
}

void init_sender_service(std::string connectionAddr)
{
    init_sender_service(rust::String(std::move(connectionAddr)));
}

void TupleBufferBuilder::set_metadata(const SerializedTupleBuffer& metaData)
{
    buffer.setSequenceNumber(NES::SequenceNumber(metaData.sequence_number));
    buffer.setChunkNumber(NES::ChunkNumber(metaData.chunk_number));
    buffer.setOriginId(NES::OriginId(metaData.origin_id));
    buffer.setLastChunk(metaData.last_chunk);
    buffer.setWatermark(NES::Timestamp(metaData.watermark));
    buffer.setNumberOfTuples(metaData.number_of_tuples);
}

void TupleBufferBuilder::set_data(rust::Slice<const uint8_t> data)
{
    INVARIANT(
        buffer.getBufferSize() >= data.length(),
        "Buffer size missmatch. Internal BufferSize: {} vs. External {}",
        buffer.getBufferSize(),
        data.length());

    memcpy(buffer.getBuffer(), data.data(), std::min(data.length(), buffer.getBufferSize()));
}

void TupleBufferBuilder::add_child_buffer(const rust::Slice<const uint8_t> child)
{
    const auto childBuffer = bufferProvider.getUnpooledBuffer(child.size());
    if (!childBuffer)
    {
        throw NES::CannotAllocateBuffer("allocating child buffer");
    }

    INVARIANT(
        childBuffer->getBufferSize() >= child.length(),
        "Unpooled Buffer size missmatch. Internal BufferSize: {} vs. External {}",
        childBuffer->getBufferSize(),
        child.length());

    memcpy(buffer.getBuffer(), child.data(), std::min(child.length(), childBuffer->getBufferSize()));
}
