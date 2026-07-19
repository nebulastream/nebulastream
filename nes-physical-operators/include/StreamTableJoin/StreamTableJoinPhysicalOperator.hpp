/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <PhysicalOperator.hpp>
#include <Watermark/TimeFunction.hpp>

namespace NES
{

/// Pipeline adapter used only to preserve the per-input pipeline boundary. The
/// adapter turns pipeline termination into empty origin-tagged buffers because
/// EOS is not passed to successor pipelines as a data event by the runtime.
class StreamTableJoinInputPhysicalOperator final : public PhysicalOperatorConcept
{
public:
    StreamTableJoinInputPhysicalOperator(OperatorHandlerId operatorHandlerId, std::vector<OriginId> inputOriginIds);

    void execute(ExecutionContext& executionCtx, Record& record) const override;
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void terminate(ExecutionContext& executionCtx) const override;

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    OperatorHandlerId operatorHandlerId;
    std::vector<OriginId> inputOriginIds;
    std::optional<PhysicalOperator> child;
};

/// A single origin-aware asymmetric stream-table join scan.
///
/// Buffers from both predecessors enter this operator. Their origin identifies
/// whether the stream or table layout is used. Table buffers grow the retained
/// state and advance its watermark; stream buffers either probe that state or
/// wait until the table watermark is sufficiently advanced.
class StreamTableJoinPhysicalOperator final : public PhysicalOperatorConcept
{
public:
    StreamTableJoinPhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        PhysicalFunction joinFunction,
        std::shared_ptr<TupleBufferRef> streamInputBufferRef,
        std::shared_ptr<TupleBufferRef> tableInputBufferRef,
        std::shared_ptr<PagedVectorTupleLayout> streamTupleLayout,
        std::shared_ptr<PagedVectorTupleLayout> tableTupleLayout,
        OriginId outputOriginId,
        std::unique_ptr<TimeFunction> streamTimeFunction = nullptr,
        std::unique_ptr<TimeFunction> tableTimeFunction = nullptr);

    StreamTableJoinPhysicalOperator(const StreamTableJoinPhysicalOperator& other);

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    void processStreamRecord(ExecutionContext& executionCtx, Record& record) const;
    void processTableRecord(ExecutionContext& executionCtx, Record& record) const;
    void probeStreamRecord(
        ExecutionContext& executionCtx,
        const Record& streamRecord,
        const nautilus::val<Timestamp>& streamTimestamp) const;
    void emitJoinedRecord(
        ExecutionContext& executionCtx,
        const Record& streamRecord,
        Record& tableRecord,
        const nautilus::val<Timestamp>& streamTimestamp) const;
    void releasePending(
        ExecutionContext& executionCtx, const nautilus::val<Timestamp>& tableWatermark, bool releaseAll) const;

    OperatorHandlerId operatorHandlerId;
    PhysicalFunction joinFunction;
    std::shared_ptr<TupleBufferRef> streamInputBufferRef;
    std::shared_ptr<TupleBufferRef> tableInputBufferRef;
    std::shared_ptr<PagedVectorTupleLayout> streamTupleLayout;
    std::shared_ptr<PagedVectorTupleLayout> tableTupleLayout;
    std::vector<Record::RecordFieldIdentifier> streamInputFields;
    std::vector<Record::RecordFieldIdentifier> tableInputFields;
    std::vector<Record::RecordFieldIdentifier> streamFields;
    std::vector<Record::RecordFieldIdentifier> tableFields;
    OriginId outputOriginId;
    std::unique_ptr<TimeFunction> streamTimeFunction;
    std::unique_ptr<TimeFunction> tableTimeFunction;
    std::optional<PhysicalOperator> child;
};

}
