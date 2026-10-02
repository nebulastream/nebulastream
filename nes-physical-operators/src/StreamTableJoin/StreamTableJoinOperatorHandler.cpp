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
#include <utility>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Interface/BTree/BTree.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sequencing/SequenceData.hpp>
#include <Time/Timestamp.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

StreamTableJoinOperatorHandler::StreamTableJoinOperatorHandler(
    const std::vector<OriginId>& tableOrigins,
    const std::vector<OriginId>& inputOrigins,
    TableIndexFactory pendingTreeFactory,
    TableIndexFactory tableIndexFactory)
    : tableIndexFactory(std::move(tableIndexFactory))
    , pendingTreeFactory(std::move(pendingTreeFactory))
    , tableOrigins(tableOrigins)
    , tableWatermarks(tableOrigins)
    , outputWatermarks(inputOrigins)
{
    PRECONDITION(!this->tableOrigins.empty(), "Stream-table join requires at least one table origin");
}

StreamTableJoinOperatorHandler::~StreamTableJoinOperatorHandler() = default;

void StreamTableJoinOperatorHandler::start(PipelineExecutionContext&)
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

TupleBuffer* StreamTableJoinOperatorHandler::getTableBufferIfExists()
{
    return tableBuffer.has_value() ? &tableBuffer.value() : nullptr;
}

TupleBuffer* StreamTableJoinOperatorHandler::getTableIndexIfExists()
{
    return tableIndex.has_value() ? &tableIndex.value() : nullptr;
}

namespace
{
TupleBuffer createPagedVectorBuffer(AbstractBufferProvider& bufferProvider, const uint64_t tupleSize)
{
    auto buffer = bufferProvider.getBufferBlocking();
    PagedVector::init(buffer, bufferProvider, tupleSize);
    return buffer;
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

TupleBuffer* StreamTableJoinOperatorHandler::getOrCreateTableIndex(AbstractBufferProvider* bufferProvider)
{
    PRECONDITION(bufferProvider != nullptr, "Buffer provider must not be null");
    PRECONDITION(static_cast<bool>(tableIndexFactory), "Stream-table join has no table index");
    if (!tableIndex.has_value())
    {
        INVARIANT(!isTableSealed(), "A sealed table must not be changed");
        tableIndex.emplace(tableIndexFactory(*bufferProvider));
    }
    return &tableIndex.value();
}

bool StreamTableJoinOperatorHandler::isTableSealed() const
{
    return tableSealed.load(std::memory_order_acquire);
}

uint64_t StreamTableJoinOperatorHandler::nextTableRowNumber()
{
    ++tableIndexRows;
    return nextTableRow++;
}

namespace
{
/// Below this size the index just grows. Kept small: rows can carry large VARSIZED values, and an ASOF join over few keys retains about
/// one row per key after a compaction.
constexpr uint64_t MIN_TABLE_INDEX_SIZE_FOR_COMPACTION = 4;
}

bool StreamTableJoinOperatorHandler::shouldCompactTableIndex(const Timestamp watermark) const
{
    if (isTableSealed() || !tableIndex.has_value() || watermark == Timestamp{Timestamp::INVALID_VALUE}
        || watermark <= tableIndexCompactionWatermark)
    {
        return false;
    }
    return tableIndexRows >= std::max(2 * tableIndexRowsAfterCompaction, MIN_TABLE_INDEX_SIZE_FOR_COMPACTION);
}

TupleBuffer* StreamTableJoinOperatorHandler::beginTableIndexCompaction(AbstractBufferProvider* bufferProvider)
{
    PRECONDITION(bufferProvider != nullptr, "Buffer provider must not be null");
    PRECONDITION(static_cast<bool>(tableIndexFactory), "Stream-table join has no table index");
    PRECONDITION(!compactedTableIndex.has_value(), "Table index compaction already in progress");
    compactedTableIndex.emplace(tableIndexFactory(*bufferProvider));
    return &compactedTableIndex.value();
}

void StreamTableJoinOperatorHandler::finishTableIndexCompaction(const Timestamp watermark, const uint64_t retainedRows)
{
    PRECONDITION(compactedTableIndex.has_value(), "No table index compaction in progress");
    tableIndex.swap(compactedTableIndex);
    compactedTableIndex.reset();
    tableIndexRows = retainedRows;
    tableIndexRowsAfterCompaction = retainedRows;
    tableIndexCompactionWatermark = watermark;
}

namespace
{
/// Below this many released rows the pending tree is not rebuilt.
constexpr uint64_t MIN_RELEASED_PENDING_ROWS_FOR_COMPACTION = 64;
}

TupleBuffer* StreamTableJoinOperatorHandler::getOrCreatePendingTree(AbstractBufferProvider* bufferProvider)
{
    PRECONDITION(bufferProvider != nullptr, "Buffer provider must not be null");
    if (!pendingTree.has_value())
    {
        pendingTree.emplace(pendingTreeFactory(*bufferProvider));
    }
    return &pendingTree.value();
}

uint64_t StreamTableJoinOperatorHandler::getReleasedPendingRows() const
{
    return releasedPendingRows;
}

bool StreamTableJoinOperatorHandler::setReleasedPendingRows(const uint64_t releasedRows)
{
    PRECONDITION(pendingTree.has_value(), "No pending tree");
    const auto size = BTree::load(pendingTree.value()).size();
    PRECONDITION(releasedRows >= releasedPendingRows && releasedRows <= size, "Released pending rows must grow within the tree");
    releasedPendingRows = releasedRows;
    return releasedRows >= MIN_RELEASED_PENDING_ROWS_FOR_COMPACTION && 2 * releasedRows >= size;
}

TupleBuffer* StreamTableJoinOperatorHandler::beginPendingCompaction(AbstractBufferProvider* bufferProvider)
{
    PRECONDITION(bufferProvider != nullptr, "Buffer provider must not be null");
    PRECONDITION(!compactedPendingTree.has_value(), "Pending compaction already in progress");
    compactedPendingTree.emplace(pendingTreeFactory(*bufferProvider));
    return &compactedPendingTree.value();
}

void StreamTableJoinOperatorHandler::finishPendingCompaction()
{
    PRECONDITION(compactedPendingTree.has_value(), "No pending compaction in progress");
    pendingTree.swap(compactedPendingTree);
    compactedPendingTree.reset();
    releasedPendingRows = 0;
}

Timestamp
StreamTableJoinOperatorHandler::updateTableWatermark(const Timestamp watermark, const SequenceData sequenceData, const OriginId originId)
{
    /// Table buffers without a watermark (no TIME clause) still count as progress at watermark zero: the processor needs every
    /// sequence number to apply the infinite watermark of the source's end-of-stream buffer.
    const auto progress = watermark == Timestamp{Timestamp::INVALID_VALUE} ? Timestamp{Timestamp::INITIAL_VALUE} : watermark;
    currentTableWatermark = tableWatermarks.updateWatermark(progress, sequenceData, originId);
    if (currentTableWatermark == Timestamp{Timestamp::INFINITE_VALUE})
    {
        tableSealed.store(true, std::memory_order_release);
    }
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
    return std::ranges::find(tableOrigins, originId) != tableOrigins.end();
}

SequenceNumber StreamTableJoinOperatorHandler::getNextOutputSequence()
{
    return SequenceNumber{nextOutputSequence.fetch_add(1, std::memory_order_relaxed)};
}

}
