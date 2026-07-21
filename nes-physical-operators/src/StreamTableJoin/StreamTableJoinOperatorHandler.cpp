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

#include <StreamTableJoin/StreamTableJoinOperatorHandler.hpp>

#include <algorithm>
#include <cstdint>
#include <utility>

#include <Interface/PagedVector/PagedVector.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

StreamTableJoinOperatorHandler::StreamTableJoinOperatorHandler(std::vector<OriginId> tableOrigins, std::vector<OriginId> inputOrigins)
    : tableOrigins(tableOrigins)
    , tableWatermarks(std::move(tableOrigins))
    , outputWatermarks(std::move(inputOrigins))
{
    PRECONDITION(!this->tableOrigins.empty(), "Stream-table join requires at least one table origin");
}

StreamTableJoinOperatorHandler::~StreamTableJoinOperatorHandler() = default;

void StreamTableJoinOperatorHandler::start(PipelineExecutionContext&, uint32_t)
{
}

void StreamTableJoinOperatorHandler::stop(QueryTerminationType, PipelineExecutionContext&)
{
}

void StreamTableJoinOperatorHandler::lock()
{
    stateMutex.lock();
}

void StreamTableJoinOperatorHandler::unlock()
{
    stateMutex.unlock();
}

namespace
{
TupleBuffer createPagedVectorBuffer(AbstractBufferProvider& bufferProvider, const uint64_t tupleSize)
{
    auto buffer = bufferProvider.getUnpooledBuffer(PagedVector::getMainBufferSize());
    if (!buffer.has_value())
    {
        throw BufferAllocationFailure("Could not allocate stream-table join state buffer");
    }
    PagedVector::init(buffer.value(), bufferProvider.getBufferSize(), tupleSize);
    return std::move(buffer.value());
}
}

TupleBuffer* StreamTableJoinOperatorHandler::getOrCreateTableBuffer(AbstractBufferProvider* bufferProvider, const uint64_t tupleSize)
{
    PRECONDITION(bufferProvider != nullptr, "Buffer provider must not be null");
    if (!tableBuffer.has_value())
    {
        tableBuffer.emplace(createPagedVectorBuffer(*bufferProvider, tupleSize));
    }
    return &tableBuffer.value();
}

TupleBuffer* StreamTableJoinOperatorHandler::getOrCreatePendingBuffer(AbstractBufferProvider* bufferProvider, const uint64_t tupleSize)
{
    PRECONDITION(bufferProvider != nullptr, "Buffer provider must not be null");
    if (!pendingBuffer.has_value())
    {
        pendingBuffer.emplace(createPagedVectorBuffer(*bufferProvider, tupleSize));
    }
    return &pendingBuffer.value();
}

TupleBuffer* StreamTableJoinOperatorHandler::beginPendingCompaction(AbstractBufferProvider* bufferProvider, const uint64_t tupleSize)
{
    PRECONDITION(bufferProvider != nullptr, "Buffer provider must not be null");
    PRECONDITION(!compactedPendingBuffer.has_value(), "Pending compaction already in progress");
    compactedPendingBuffer.emplace(createPagedVectorBuffer(*bufferProvider, tupleSize));
    compactedPendingTimestamps.clear();
    return &compactedPendingBuffer.value();
}

void StreamTableJoinOperatorHandler::appendPendingTimestamp(const uint64_t timestamp)
{
    pendingTimestamps.push_back(timestamp);
}

void StreamTableJoinOperatorHandler::appendCompactedPendingTimestamp(const uint64_t timestamp)
{
    compactedPendingTimestamps.push_back(timestamp);
}

void StreamTableJoinOperatorHandler::finishPendingCompaction()
{
    PRECONDITION(compactedPendingBuffer.has_value(), "No pending compaction in progress");
    pendingBuffer.swap(compactedPendingBuffer);
    compactedPendingBuffer.reset();
    pendingTimestamps.swap(compactedPendingTimestamps);
    compactedPendingTimestamps.clear();
}

uint64_t StreamTableJoinOperatorHandler::getPendingTimestamp(const uint64_t index) const
{
    return pendingTimestamps.at(index);
}

uint64_t StreamTableJoinOperatorHandler::getNumberOfPendingRows() const
{
    return pendingTimestamps.size();
}

Timestamp
StreamTableJoinOperatorHandler::updateTableWatermark(const Timestamp watermark, const SequenceData sequenceData, const OriginId originId)
{
    currentTableWatermark = tableWatermarks.updateWatermark(watermark, sequenceData, originId);
    tableComplete = currentTableWatermark == Timestamp{Timestamp::INVALID_VALUE};
    return currentTableWatermark;
}

Timestamp StreamTableJoinOperatorHandler::getTableWatermark() const
{
    return currentTableWatermark;
}

Timestamp StreamTableJoinOperatorHandler::getOutputWatermark() const
{
    return outputWatermarks.getCurrentWatermark();
}

Timestamp
StreamTableJoinOperatorHandler::updateOutputWatermark(const Timestamp watermark, const SequenceData sequenceData, const OriginId originId)
{
    return outputWatermarks.updateWatermark(watermark, sequenceData, originId);
}

bool StreamTableJoinOperatorHandler::isTableComplete() const
{
    return tableComplete;
}

bool StreamTableJoinOperatorHandler::isTableOrigin(const OriginId originId) const
{
    return std::ranges::contains(tableOrigins, originId);
}

void StreamTableJoinOperatorHandler::observeInputSequence(const OriginId originId, const SequenceNumber sequenceNumber)
{
    const auto entry = std::ranges::find(lastInputSequences, originId, &std::pair<OriginId, SequenceNumber>::first);
    if (entry == lastInputSequences.end())
    {
        lastInputSequences.emplace_back(originId, sequenceNumber);
    }
    else if (entry->second < sequenceNumber)
    {
        entry->second = sequenceNumber;
    }
}

SequenceNumber StreamTableJoinOperatorHandler::getNextInputSequence(const OriginId originId) const
{
    const auto entry = std::ranges::find(lastInputSequences, originId, &std::pair<OriginId, SequenceNumber>::first);
    if (entry == lastInputSequences.end())
    {
        return SequenceNumber{SequenceNumber::INITIAL};
    }
    return SequenceNumber{entry->second.getRawValue() + 1};
}

SequenceNumber StreamTableJoinOperatorHandler::getNextOutputSequence()
{
    return SequenceNumber{nextOutputSequence.fetch_add(1, std::memory_order_relaxed)};
}

}
