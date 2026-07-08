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
#include <mutex>

#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <folly/Synchronized.h>

namespace NES
{


namespace detail
{

struct SNBufferingRecord
{
    std::vector<TupleBuffer> buffers = std::vector<TupleBuffer>();
    size_t iterator = 0;
    ChunkNumber maxChunkNumber = INVALID_CHUNK_NUMBER;
    Epoch originEpoch = INVALID_EPOCH;
    bool completed = false; /// completed = all chunks have been received
    bool finalized = false; /// finalized = all chunks have been retrieved and the record is ready for garbage collection
};

struct WatermarkedSNMap
{
    /// lowWatermark is the highest sequence number for which all previous sequence numbers have been completed
    /// sequence numbers below the lawWatermark may not be present in the map anymore
    SequenceNumber lowWatermark = INVALID_SEQ_NUMBER;
    std::unordered_map<SequenceNumber, folly::Synchronized<detail::SNBufferingRecord>> records;
};
}

class SNDeduplicationOperatorHandler : public OperatorHandler
{
public:
    SNDeduplicationOperatorHandler(std::string filePath, const std::vector<OriginId>& inputOrigins);

    void start(PipelineExecutionContext&, uint32_t) override {

    };

    void stop(QueryTerminationType, PipelineExecutionContext&) override { }

    void init();

    void save();

    /// apply sequence number based deduplication
    /// will buffer chunks for a sequence number until all chunks have arrived
    /// return true when all chunks are there and the SND operator can retrieve them
    bool filterAndBuffer(TupleBuffer* buffer);

    /// retrieve single chunk for the buffer's sequence number and progresses an internal iterator
    /// returns nullptr when there are no more chunks
    TupleBuffer* probe(TupleBuffer* buffer);

    /// after the last chunk has been retrieved, prepare the sequence number for garbage collection
    /// attempts to run garbage collection synchronously
    void finalizeSN(TupleBuffer* buffer);


private:
    const std::string filePath;
    const std::vector<OriginId> origins;
    std::unordered_map<OriginId, std::unique_ptr<std::mutex>> gcMutexes;
    std::unordered_map<OriginId, folly::Synchronized<detail::WatermarkedSNMap>> buffers;
};

}