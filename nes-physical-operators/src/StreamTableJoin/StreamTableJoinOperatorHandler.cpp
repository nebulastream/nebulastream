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
#include <atomic>
#include <cstdint>
#include <iterator>
#include <numeric>
#include <utility>
#include <vector>

#include <Interface/PagedVector/PagedVector.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <bits/ranges_algo.h>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include "Identifiers/Identifiers.hpp"
#include "Sequencing/SequenceData.hpp"
#include "Time/Timestamp.hpp"

namespace NES
{

StreamTableJoinOperatorHandler::StreamTableJoinOperatorHandler(
    const std::vector<OriginId>& tableOrigins, const std::vector<OriginId>& inputOrigins)
    : tableOrigins(tableOrigins), tableWatermarks(tableOrigins), outputWatermarks(inputOrigins)
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

TupleBuffer* StreamTableJoinOperatorHandler::beginTableCompaction(AbstractBufferProvider* bufferProvider, const uint64_t tupleSize)
{
    PRECONDITION(bufferProvider != nullptr, "Buffer provider must not be null");
    PRECONDITION(!compactedTableBuffer.has_value(), "Table compaction already in progress");
    compactedTableBuffer.emplace(createPagedVectorBuffer(*bufferProvider, tupleSize));
    compactedTableTimestamps.clear();
    return &compactedTableBuffer.value();
}

void StreamTableJoinOperatorHandler::appendTableTimestamp(const uint64_t timestamp)
{
    tableTimestamps.push_back(timestamp);
    tableTimestampOrderDirty = true;
}

void StreamTableJoinOperatorHandler::appendCompactedTableTimestamp(const uint64_t timestamp)
{
    compactedTableTimestamps.push_back(timestamp);
}

void StreamTableJoinOperatorHandler::finishTableCompaction()
{
    PRECONDITION(compactedTableBuffer.has_value(), "No table compaction in progress");
    tableBuffer.swap(compactedTableBuffer);
    compactedTableBuffer.reset();
    tableTimestamps.swap(compactedTableTimestamps);
    compactedTableTimestamps.clear();
    tableTimestampOrderDirty = true;
}

void StreamTableJoinOperatorHandler::prepareTableTimestampOrder()
{
    if (!tableTimestampOrderDirty)
    {
        return;
    }
    tableTimestampOrder.resize(tableTimestamps.size());
    std::ranges::iota(tableTimestampOrder, uint64_t{0});
    std::ranges::sort(
        tableTimestampOrder,
        [&](const uint64_t left, const uint64_t right)
        {
            if (tableTimestamps.at(left) != tableTimestamps.at(right))
            {
                return tableTimestamps.at(left) < tableTimestamps.at(right);
            }
            return left > right;
        });
    tableTimestampOrderDirty = false;
}

uint64_t StreamTableJoinOperatorHandler::getTableTimestamp(const uint64_t index) const
{
    return tableTimestamps.at(index);
}

uint64_t StreamTableJoinOperatorHandler::getDescendingTimestampStartPosition(const uint64_t timestamp) const
{
    PRECONDITION(!tableTimestampOrderDirty, "Timestamp order must be prepared before probing");
    const auto firstFutureTimestamp = std::ranges::upper_bound(
        tableTimestampOrder,

        timestamp,
        [&](const uint64_t candidateTimestamp, const uint64_t rowIndex) { return candidateTimestamp < tableTimestamps.at(rowIndex); });
    return tableTimestampOrder.size() - static_cast<uint64_t>(std::distance(tableTimestampOrder.begin(), firstFutureTimestamp));
}

uint64_t StreamTableJoinOperatorHandler::getTableIndexByDescendingTimestampPosition(const uint64_t position) const
{
    return tableTimestampOrder.at(tableTimestampOrder.size() - 1 - position);
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
    if (watermark == Timestamp{Timestamp::INVALID_VALUE})
    {
        return currentTableWatermark;
    }
    currentTableWatermark = tableWatermarks.updateWatermark(watermark, sequenceData, originId);
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
    if (watermark == Timestamp{Timestamp::INVALID_VALUE})
    {
        return outputWatermarks.getCurrentWatermark();
    }
    return outputWatermarks.updateWatermark(watermark, sequenceData, originId);
}

bool StreamTableJoinOperatorHandler::isTableOrigin(const OriginId originId) const
{
    return std::ranges::contains(tableOrigins, originId);
}

SequenceNumber StreamTableJoinOperatorHandler::getNextOutputSequence()
{
    return SequenceNumber{nextOutputSequence.fetch_add(1, std::memory_order_relaxed)};
}

}
