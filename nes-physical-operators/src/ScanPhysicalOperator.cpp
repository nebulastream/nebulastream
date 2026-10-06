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


#include <ScanPhysicalOperator.hpp>

#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <ExecutionContext.hpp>
#include <InputFormatter.hpp>
#include <PhysicalOperator.hpp>
#include <PipelineState.hpp>
#include <SequenceShredder.hpp>
#include <function.hpp>
#include <val.hpp>
#include <val_arith.hpp>

namespace NES
{

void emitStagedBuffer(PipelineStateBuilder* state, const StagedBuffer& staged)
{
    const auto& raw = staged.getRawTupleBuffer().getRawBuffer();
    const auto present = static_cast<uint64_t>(raw.getBufferSize() != 0);
    state->append(present);
    if (present != 0)
    {
        state->append(staged.getOffsetOfLastTuple());
        state->append(staged.getByteOffsetOfLastTuple());
        state->addChild(raw);
    }
}

StagedBuffer absorbStagedBuffer(PipelineStateReader* state, uint64_t& nextBuffer)
{
    const auto present = state->read<uint64_t>();
    INVARIANT(present <= 1, "Invalid staged-buffer marker");
    if (present == 0)
    {
        return {};
    }
    const auto first = state->read<FieldIndex>();
    const auto last = state->read<FieldIndex>();
    return StagedBuffer{RawTupleBuffer{state->child(nextBuffer++)}, first, last};
}

void emitScanState(InputFormatter* formatter, PipelineStateBuilder* state)
{
    const auto snapshot = formatter->snapshotShredder();
    state->append(static_cast<uint64_t>(snapshot.entries.size()));
    for (const auto& entry : snapshot.entries)
    {
        state->append(entry.sequenceNumber);
        const uint64_t flags = static_cast<uint64_t>(entry.hasDelimiter) | (static_cast<uint64_t>(entry.leadingUsed) << 1)
            | (static_cast<uint64_t>(entry.trailingUsed) << 2);
        state->append(flags);
        emitStagedBuffer(state, entry.leading);
        emitStagedBuffer(state, entry.trailing);
    }
    state->append(static_cast<uint64_t>(snapshot.emptyRanges.size()));
    for (const auto& [first, last] : snapshot.emptyRanges)
    {
        state->append(first);
        state->append(last);
    }
}

uint64_t absorbScanState(InputFormatter* formatter, PipelineStateReader* state, const bool hasChild)
{
    SequenceShredder::Snapshot snapshot;
    uint64_t nextBuffer = 0;
    const auto entries = state->read<uint64_t>();
    snapshot.entries.reserve(entries);
    for (uint64_t index = 0; index < entries; ++index)
    {
        const auto sequenceNumber = state->read<uint64_t>();
        const auto flags = state->read<uint64_t>();
        INVARIANT((flags & ~uint64_t{7}) == 0, "Invalid shredder entry flags");
        auto leading = absorbStagedBuffer(state, nextBuffer);
        auto trailing = absorbStagedBuffer(state, nextBuffer);
        snapshot.entries.push_back(
            {.sequenceNumber = sequenceNumber,
             .leading = std::move(leading),
             .trailing = std::move(trailing),
             .hasDelimiter = static_cast<bool>(flags & 1),
             .leadingUsed = static_cast<bool>(flags & 2),
             .trailingUsed = static_cast<bool>(flags & 4)});
    }
    const auto ranges = state->read<uint64_t>();
    snapshot.emptyRanges.reserve(ranges);
    for (uint64_t index = 0; index < ranges; ++index)
    {
        const auto first = state->read<uint64_t>();
        const auto last = state->read<uint64_t>();
        snapshot.emptyRanges.emplace_back(first, last);
    }
    INVARIANT(state->childCount() == nextBuffer + static_cast<uint64_t>(hasChild), "Unexpected scan state children");
    formatter->restoreShredder(std::move(snapshot));
    return nextBuffer;
}

PipelineStateReader* createScanChildReader(PipelineStateReader* state, const uint64_t childIndex)
{
    return state->own(std::make_unique<PipelineStateReader>(state->child(childIndex)));
}

void finishScanChildReader(PipelineStateReader* state, PipelineStateReader* child)
{
    child->ensureConsumed();
    state->ensureConsumed();
}

void finishScanLeafReader(PipelineStateReader* state)
{
    state->ensureConsumed();
}

ScanPhysicalOperator::ScanPhysicalOperator(
    std::shared_ptr<TupleBufferRef> bufferRef, std::vector<Record::RecordFieldIdentifier> projections)
    : bufferRef(std::move(bufferRef))
    , projections(std::move(projections))
    , isRawScan(std::dynamic_pointer_cast<InputFormatter>(this->bufferRef) != nullptr)
{
}

void ScanPhysicalOperator::rawScan(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    auto inputFormatterBufferRef = std::dynamic_pointer_cast<InputFormatter>(this->bufferRef);

    if (not inputFormatterBufferRef->indexBuffer(recordBuffer, executionCtx.pipelineMemoryProvider.arena))
    {
        executionCtx.setOpenReturnState(OpenReturnState::REPEAT);
        return;
    }

    /// call open on all child operators
    openChild(executionCtx, recordBuffer);

    /// process buffer
    const auto executeChildLambda = [this](ExecutionContext& executionCtx, Record& record) { executeChild(executionCtx, record); };
    inputFormatterBufferRef->readBuffer(executionCtx, recordBuffer, executeChildLambda);
}

void ScanPhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// initialize global state variables to keep track of the watermark ts and the origin id
    executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
    executionCtx.originId = recordBuffer.getOriginId();
    executionCtx.currentTs = recordBuffer.getCreatingTs();
    executionCtx.sequenceNumber = recordBuffer.getSequenceNumber();
    executionCtx.chunkNumber = recordBuffer.getChunkNumber();
    executionCtx.lastChunk = recordBuffer.isLastChunk();

    if (isRawScan)
    {
        rawScan(executionCtx, recordBuffer);
        return;
    }
    /// call open on all child operators
    openChild(executionCtx, recordBuffer);
    /// iterate over records in buffer
    auto numberOfRecords = recordBuffer.getNumRecords();
    for (nautilus::val<uint64_t> i = uint64_t{0}; i < numberOfRecords; i = i + uint64_t{1})
    {
        auto record = bufferRef->readRecord(projections, recordBuffer, i);
        executeChild(executionCtx, record);
    }
}

void ScanPhysicalOperator::emit(PipelineStateBuilder& state, PipelineExecutionContext& context) const
{
    if (isRawScan)
    {
        emitScanState(dynamic_cast<InputFormatter*>(bufferRef.get()), &state);
    }
    PhysicalOperatorConcept::emit(state, context);
}

void ScanPhysicalOperator::absorb(PipelineStateReader& state, PipelineExecutionContext& context) const
{
    if (not isRawScan)
    {
        PhysicalOperatorConcept::absorb(state, context);
        return;
    }
    const auto childIndex = absorbScanState(dynamic_cast<InputFormatter*>(bufferRef.get()), &state, getChild().has_value());
    if (const auto next = getChild())
    {
        auto childBuffer = state.child(childIndex);
        PipelineStateReader childState(childBuffer);
        next->absorb(childState, context);
        childState.ensureConsumed();
    }
    state.ensureConsumed();
}

void ScanPhysicalOperator::lowerEmit(nautilus::val<PipelineStateBuilder*> state, nautilus::val<PipelineExecutionContext*> context) const
{
    if (isRawScan)
    {
        nautilus::invoke(emitScanState, nautilus::val<InputFormatter*>{dynamic_cast<InputFormatter*>(bufferRef.get())}, state);
    }
    PhysicalOperatorConcept::lowerEmit(state, context);
}

void ScanPhysicalOperator::lowerAbsorb(nautilus::val<PipelineStateReader*> state, nautilus::val<PipelineExecutionContext*> context) const
{
    if (not isRawScan)
    {
        PhysicalOperatorConcept::lowerAbsorb(state, context);
        return;
    }
    const auto childIndex = nautilus::invoke(
        absorbScanState,
        nautilus::val<InputFormatter*>{dynamic_cast<InputFormatter*>(bufferRef.get())},
        state,
        nautilus::val<bool>{getChild().has_value()});
    if (const auto next = getChild())
    {
        const auto childState = nautilus::invoke(createScanChildReader, state, childIndex);
        next->lowerAbsorb(childState, context);
        nautilus::invoke(finishScanChildReader, state, childState);
    }
    else
    {
        nautilus::invoke(finishScanLeafReader, state);
    }
}

std::optional<PhysicalOperator> ScanPhysicalOperator::getChild() const
{
    return child;
}

void ScanPhysicalOperator::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}

}
