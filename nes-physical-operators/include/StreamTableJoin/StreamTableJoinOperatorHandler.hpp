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

#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sequencing/SequenceData.hpp>
#include <Time/Timestamp.hpp>
#include <Watermark/MultiOriginWatermarkProcessor.hpp>

namespace NES
{
class AbstractBufferProvider;
class PipelineExecutionContext;

/// Shared state for the two input pipelines of a stream-table join.
///
/// While the table can still change, all state access holds the state lock, once per
/// buffer: insertion, watermark advancement, probing, pending-row release and
/// compaction are each one atomic step. Once the table watermark is infinite, every table source has ended and
/// all of its buffers were inserted: the table is sealed and read-only, and stream
/// buffers probe it without the lock.
class StreamTableJoinOperatorHandler final : public OperatorHandler
{
public:
    /// Creates an empty index: a chained hash map or B-tree for the table, a B-tree for the pending stream rows.
    using TableIndexFactory = std::function<TupleBuffer(AbstractBufferProvider&)>;

    /// With a `tableIndexFactory`, the table is kept in that index instead of a paged vector. `pendingTreeFactory` creates the B-tree
    /// for pending stream rows.
    StreamTableJoinOperatorHandler(
        const std::vector<OriginId>& tableOrigins,
        const std::vector<OriginId>& inputOrigins,
        TableIndexFactory pendingTreeFactory,
        ///NOLINTNEXTLINE(fuchsia-default-arguments-declarations)
        TableIndexFactory tableIndexFactory = {});
    ~StreamTableJoinOperatorHandler() override;

    void start(PipelineExecutionContext& pipelineExecutionContext) override;
    void stop(QueryTerminationType terminationType, PipelineExecutionContext& pipelineExecutionContext) override;

    void lock();
    void unlock();

    TupleBuffer* getOrCreateTableBuffer(AbstractBufferProvider* bufferProvider, uint64_t tupleSize);
    TupleBuffer* getOrCreateTableIndex(AbstractBufferProvider* bufferProvider);
    /// For probes, which also run without the lock once the table is sealed and so must not create state: nullptr while the table is empty.
    [[nodiscard]] TupleBuffer* getTableBufferIfExists();
    [[nodiscard]] TupleBuffer* getTableIndexIfExists();
    /// True once the table watermark is infinite. The table state must not change afterwards, so it can be probed without the lock.
    [[nodiscard]] bool isTableSealed() const;
    /// Insertion number of the next table row; orders rows with equal timestamps (ASOF: the first inserted row wins). Also counts the
    /// rows in the index for shouldCompactTableIndex, so every inserted row must draw a number.
    [[nodiscard]] uint64_t nextTableRowNumber();
    /// True if the table index should be rebuilt without the rows that `watermark` made obsolete: the table is not sealed, the watermark
    /// moved since the last rebuild, and the index has at least doubled since then. Doubling keeps the rebuilds amortized O(1) per row.
    [[nodiscard]] bool shouldCompactTableIndex(Timestamp watermark) const;
    TupleBuffer* beginTableIndexCompaction(AbstractBufferProvider* bufferProvider);
    void finishTableIndexCompaction(Timestamp watermark, uint64_t retainedRows);
    /// Pending stream rows wait in a B-tree ordered by timestamp. A row only waits if its timestamp is at or after the table watermark,
    /// and that watermark only grows, so the rows a release may take are always a prefix of the tree and new rows sort after every
    /// released row. The tree therefore needs no deletion: a counter marks how much of the prefix is released, and the tree is rebuilt
    /// from the waiting rows once released rows make up half of it.
    TupleBuffer* getOrCreatePendingTree(AbstractBufferProvider* bufferProvider);
    [[nodiscard]] uint64_t getReleasedPendingRows() const;
    /// Marks the first `releasedRows` rows of the pending tree as released. Returns true if the tree should now be rebuilt without them.
    [[nodiscard]] bool setReleasedPendingRows(uint64_t releasedRows);
    TupleBuffer* beginPendingCompaction(AbstractBufferProvider* bufferProvider);
    void finishPendingCompaction();

    [[nodiscard]] Timestamp updateTableWatermark(Timestamp watermark, SequenceData sequenceData, OriginId originId);
    [[nodiscard]] Timestamp updateOutputWatermark(Timestamp watermark, SequenceData sequenceData, OriginId originId);
    [[nodiscard]] Timestamp getTableWatermark() const;
    [[nodiscard]] Timestamp getOutputWatermark() const;
    [[nodiscard]] bool isTableOrigin(OriginId originId) const;
    [[nodiscard]] SequenceNumber getNextOutputSequence();

private:
    std::mutex stateMutex;
    std::optional<TupleBuffer> tableBuffer;
    TableIndexFactory tableIndexFactory;
    std::optional<TupleBuffer> tableIndex;
    std::optional<TupleBuffer> compactedTableIndex;
    uint64_t nextTableRow{0};
    uint64_t tableIndexRows{0};
    uint64_t tableIndexRowsAfterCompaction{0};
    Timestamp tableIndexCompactionWatermark{Timestamp::INITIAL_VALUE};
    std::atomic_bool tableSealed{false};

    TableIndexFactory pendingTreeFactory;
    std::optional<TupleBuffer> pendingTree;
    std::optional<TupleBuffer> compactedPendingTree;
    uint64_t releasedPendingRows{0};
    std::vector<OriginId> tableOrigins;
    MultiOriginWatermarkProcessor tableWatermarks;
    MultiOriginWatermarkProcessor outputWatermarks;
    Timestamp currentTableWatermark{Timestamp::INITIAL_VALUE};
    std::atomic_uint64_t nextOutputSequence{SequenceNumber::INITIAL};
};
}
