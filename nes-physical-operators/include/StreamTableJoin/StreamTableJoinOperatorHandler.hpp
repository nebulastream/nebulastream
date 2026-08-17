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
#include <mutex>
#include <optional>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sequencing/SequenceData.hpp>
#include <Time/Timestamp.hpp>
#include <Watermark/MultiOriginWatermarkProcessor.hpp>
#include "Runtime/QueryTerminationType.hpp"

namespace NES
{
class AbstractBufferProvider;
class PipelineExecutionContext;

/// Shared state for the two input pipelines of a stream-table join.
///
/// State access is intentionally serialized in the first implementation. This
/// makes insertion, watermark advancement and pending-row release one atomic
/// operation. The state representation can later be replaced by partitioned
/// hash indexes without changing the physical operators' protocol.
class StreamTableJoinOperatorHandler final : public OperatorHandler
{
public:
    StreamTableJoinOperatorHandler(const std::vector<OriginId>& tableOrigins, const std::vector<OriginId>& inputOrigins);
    ~StreamTableJoinOperatorHandler() override;

    void start(PipelineExecutionContext& pipelineExecutionContext, uint32_t localStateVariableId) override;
    void stop(QueryTerminationType terminationType, PipelineExecutionContext& pipelineExecutionContext) override;

    void lock();
    void unlock();

    TupleBuffer* getOrCreateTableBuffer(AbstractBufferProvider* bufferProvider, uint64_t tupleSize);
    TupleBuffer* beginTableCompaction(AbstractBufferProvider* bufferProvider, uint64_t tupleSize);
    void appendTableTimestamp(uint64_t timestamp);
    void appendCompactedTableTimestamp(uint64_t timestamp);
    void finishTableCompaction();
    [[nodiscard]] uint64_t getTableTimestamp(uint64_t index) const;
    void prepareTableTimestampOrder();
    [[nodiscard]] uint64_t getDescendingTimestampStartPosition(uint64_t timestamp) const;
    [[nodiscard]] uint64_t getTableIndexByDescendingTimestampPosition(uint64_t position) const;
    TupleBuffer* getOrCreatePendingBuffer(AbstractBufferProvider* bufferProvider, uint64_t tupleSize);
    TupleBuffer* beginPendingCompaction(AbstractBufferProvider* bufferProvider, uint64_t tupleSize);

    void appendPendingTimestamp(uint64_t timestamp);
    void appendCompactedPendingTimestamp(uint64_t timestamp);
    void finishPendingCompaction();
    [[nodiscard]] uint64_t getPendingTimestamp(uint64_t index) const;
    [[nodiscard]] uint64_t getNumberOfPendingRows() const;

    [[nodiscard]] Timestamp updateTableWatermark(Timestamp watermark, SequenceData sequenceData, OriginId originId);
    [[nodiscard]] Timestamp updateOutputWatermark(Timestamp watermark, SequenceData sequenceData, OriginId originId);
    [[nodiscard]] Timestamp getTableWatermark() const;
    [[nodiscard]] Timestamp getOutputWatermark() const;
    [[nodiscard]] bool isTableOrigin(OriginId originId) const;
    [[nodiscard]] SequenceNumber getNextOutputSequence();

private:
    std::recursive_mutex stateMutex;
    std::optional<TupleBuffer> tableBuffer;
    std::optional<TupleBuffer> compactedTableBuffer;
    std::vector<uint64_t> tableTimestamps;
    /// Row indices ordered by timestamp ascending. Equal timestamps are ordered by row index descending, so reverse traversal preserves
    /// the original ASOF tie-break (the first row in state wins).
    std::vector<uint64_t> tableTimestampOrder;
    bool tableTimestampOrderDirty{false};
    std::vector<uint64_t> compactedTableTimestamps;
    std::optional<TupleBuffer> pendingBuffer;
    std::optional<TupleBuffer> compactedPendingBuffer;
    /// Only stream rows still waiting for the table watermark are retained.
    /// A release pass compacts these timestamps together with pendingBuffer.
    std::vector<uint64_t> pendingTimestamps;
    std::vector<uint64_t> compactedPendingTimestamps;
    std::vector<OriginId> tableOrigins;
    MultiOriginWatermarkProcessor tableWatermarks;
    MultiOriginWatermarkProcessor outputWatermarks;
    Timestamp currentTableWatermark{Timestamp::INITIAL_VALUE};
    std::atomic_uint64_t nextOutputSequence{SequenceNumber::INITIAL};
};
}
