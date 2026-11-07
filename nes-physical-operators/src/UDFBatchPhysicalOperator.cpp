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

#include <UDFBatchPhysicalOperator.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <memory>
#include <optional>
#include <ranges>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Interface/BufferRef/ArrowBufferRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <nautilus/function.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <OperatorState.hpp>
#include <PhysicalOperator.hpp>
#include <UdfAbi.h>
#include <UdfDescriptor.hpp>
#include <UdfInterpreter.hpp>
#include <UdfStage.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{
using Location = ArrowBufferRef::Location;
using Region = ArrowBufferRef::Region;

/// The batch that execute fills, and how many records it holds.
class UdfBatchState : public OperatorState
{
public:
    explicit UdfBatchState(const RecordBuffer& batch) : batch(batch) { }

    RecordBuffer batch;
    nautilus::val<uint64_t> numberOfRecords = 0;
};

/// The argument columns of one UDF as an Arrow struct array that points into the batch without owning it: releasing it frees
/// only the arrays themselves.
struct ExportedColumns
{
    std::vector<ArrowArray> children;
    std::vector<ArrowArray*> childPointers;
    std::vector<std::array<const void*, 3>> buffers;
    std::array<const void*, 1> structBuffers{nullptr};
};

/// Arrow allows a null values buffer only for an empty array, so a binary column without values points here.
constexpr std::byte NO_VALUES{};

/// NOLINTBEGIN(cppcoreguidelines-owning-memory)
void releaseColumn(ArrowArray* array)
{
    array->release = nullptr;
}

void releaseColumns(ArrowArray* array)
{
    auto* const columns = static_cast<ExportedColumns*>(array->private_data);
    for (auto& child : columns->children)
    {
        if (child.release != nullptr)
        {
            child.release(&child);
        }
    }
    delete columns;
    array->release = nullptr;
}

/// NOLINTEND(cppcoreguidelines-owning-memory)

ArrowArray exportColumns(const TupleBuffer& buffer, const ArrowBufferRef& layout, const std::vector<size_t>& columns, const int64_t rows)
{
    auto exported = std::make_unique<ExportedColumns>();
    exported->children.resize(columns.size());
    exported->buffers.resize(columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
    {
        const auto column = columns[i];
        auto& buffers = exported->buffers[i];
        buffers[0] = ArrowBufferRef::resolve(buffer, layout.getLocation(buffer, column, Region::VALIDITY));
        buffers[1] = ArrowBufferRef::resolve(buffer, layout.getLocation(buffer, column, Region::DATA));
        int64_t numberOfBuffers = 2;
        if (layout.getFields()[column].type.type == DataType::Type::VARSIZED)
        {
            const auto* const values = ArrowBufferRef::resolve(buffer, layout.getLocation(buffer, column, Region::VALUES));
            buffers[2] = values != nullptr ? values : &NO_VALUES;
            numberOfBuffers = 3;
        }
        exported->children[i] = ArrowArray{
            .length = rows,
            /// Unknown: the consumer counts the NULLs if it needs to.
            .null_count = buffers[0] == nullptr ? 0 : -1,
            .offset = 0,
            .n_buffers = numberOfBuffers,
            .n_children = 0,
            .buffers = buffers.data(),
            .children = nullptr,
            .dictionary = nullptr,
            .release = &releaseColumn,
            .private_data = nullptr};
        exported->childPointers.push_back(&exported->children[i]);
    }
    return ArrowArray{
        .length = rows,
        .null_count = 0,
        .offset = 0,
        .n_buffers = 1,
        .n_children = static_cast<int64_t>(columns.size()),
        .buffers = exported->structBuffers.data(),
        .children = exported->childPointers.data(),
        .dictionary = nullptr,
        .release = &releaseColumns,
        .private_data = exported.release()};
}

/// Copies the array that UDF returned into its result column of the batch. Its validity and data go where the layout reserved
/// room for them in the batch itself, the values of a VARSIZED result into a child buffer.
void importColumn(
    const ArrowArray& result, TupleBuffer& buffer, const ArrowBufferRef& layout, const size_t column, AbstractBufferProvider& bufferProvider)
{
    const auto& field = layout.getFields()[column];
    const auto rows = static_cast<int64_t>(buffer.getNumberOfTuples());
    const bool binary = field.type.type == DataType::Type::VARSIZED;
    if (result.length != rows || result.offset != 0 || result.n_buffers != (binary ? 3 : 2))
    {
        throw UdfExecutionError(
            "UDF returned an array of {} rows at offset {} with {} buffers for a batch of {} rows",
            result.length,
            result.offset,
            result.n_buffers,
            rows);
    }
    INVARIANT(field.bitmapOffset != ArrowBufferRef::Field::NO_BITMAP, "The result column {} must be nullable", field.name);

    const auto memory = buffer.getAvailableMemoryArea();
    const auto bitmapBytes = static_cast<size_t>((rows + 7) / 8);
    if (result.buffers[0] != nullptr && result.null_count != 0)
    {
        std::memcpy(memory.subspan(field.bitmapOffset).data(), result.buffers[0], bitmapBytes);
        layout.setLocation(buffer, column, Region::VALIDITY, {.buffer = Location::MAIN_BUFFER, .offset = static_cast<uint32_t>(field.bitmapOffset)});
    }
    else
    {
        layout.setLocation(buffer, column, Region::VALIDITY, {.buffer = Location::NO_BUFFER, .offset = 0});
    }

    const auto dataBytes = [&] -> size_t
    {
        switch (field.type.type)
        {
            case DataType::Type::BOOLEAN:
                return bitmapBytes;
            case DataType::Type::VARSIZED:
                return static_cast<size_t>(rows + 1) * sizeof(int32_t);
            default:
                return static_cast<size_t>(rows) * field.type.getSizeInBytesWithoutNull();
        }
    }();
    std::memcpy(memory.subspan(field.dataOffset).data(), result.buffers[1], dataBytes);
    layout.setLocation(buffer, column, Region::DATA, {.buffer = Location::MAIN_BUFFER, .offset = static_cast<uint32_t>(field.dataOffset)});
    layout.setLocation(buffer, column, Region::VALUES, {.buffer = Location::NO_BUFFER, .offset = 0});
    if (!binary)
    {
        return;
    }

    int32_t valuesBytes = 0;
    std::memcpy(&valuesBytes, static_cast<const std::byte*>(result.buffers[1]) + (rows * static_cast<int64_t>(sizeof(int32_t))), sizeof(int32_t));
    if (valuesBytes <= 0)
    {
        return;
    }
    auto values = [&]
    {
        if (static_cast<uint64_t>(valuesBytes) <= bufferProvider.getBufferSize())
        {
            return bufferProvider.getBufferBlocking();
        }
        auto unpooled = bufferProvider.getUnpooledBuffer(static_cast<size_t>(valuesBytes));
        if (!unpooled.has_value())
        {
            throw CannotAllocateBuffer("Cannot allocate unpooled buffer of size {}", valuesBytes);
        }
        return *unpooled;
    }();
    std::memcpy(values.getAvailableMemoryArea().data(), result.buffers[2], static_cast<size_t>(valuesBytes));
    const auto childIndex = buffer.storeChildBuffer(values);
    layout.setLocation(buffer, column, Region::VALUES, {.buffer = childIndex.getRawValue() + 1, .offset = 0});
}

/// Releases an array the bridge returned, also if importing it throws.
class ReleaseOnExit
{
public:
    explicit ReleaseOnExit(ArrowArray& array) : array(array) { }

    ~ReleaseOnExit()
    {
        if (array.release != nullptr)
        {
            array.release(&array);
        }
    }

    ReleaseOnExit(const ReleaseOnExit&) = delete;
    ReleaseOnExit& operator=(const ReleaseOnExit&) = delete;
    ReleaseOnExit(ReleaseOnExit&&) = delete;
    ReleaseOnExit& operator=(ReleaseOnExit&&) = delete;

private:
    ArrowArray& array;
};

/// The stage throws UdfExecutionError on a recoverable failure; the throw unwinds out of nautilus::invoke and fails only the
/// query (the worker survives).
void runUdfs(const UDFBatchPhysicalOperator::Batch* batch, TupleBuffer* buffer, const uint64_t rows, AbstractBufferProvider* bufferProvider)
{
    buffer->setNumberOfTuples(rows);
    if (batch->stage->isFused())
    {
        auto arguments = exportColumns(*buffer, *batch->layout, batch->allArgumentColumns, static_cast<int64_t>(rows));
        ArrowArray result{};
        batch->stage->executeFusedBatch(&arguments, &result);
        const ReleaseOnExit release{result};
        if (result.n_children != static_cast<int64_t>(batch->resultColumns.size()))
        {
            throw UdfExecutionError("Fused UDFs returned {} result columns for {} UDFs", result.n_children, batch->resultColumns.size());
        }
        for (size_t udf = 0; udf < batch->resultColumns.size(); ++udf)
        {
            importColumn(*result.children[udf], *buffer, *batch->layout, batch->resultColumns[udf], *bufferProvider);
        }
        return;
    }
    for (size_t udf = 0; udf < batch->resultColumns.size(); ++udf)
    {
        auto arguments = exportColumns(*buffer, *batch->layout, batch->argumentColumns[udf], static_cast<int64_t>(rows));
        ArrowArray result{};
        batch->stage->executeBatch(udf, &arguments, &result);
        const ReleaseOnExit release{result};
        importColumn(result, *buffer, *batch->layout, batch->resultColumns[udf], *bufferProvider);
    }
}

/// Keeps a processed batch alive with the next one: the records of a batch can reach operators that keep pointing into it, such
/// as an emit that copies VARSIZED values only when its buffer is complete.
void keepAlive(TupleBuffer* next, TupleBuffer* processed)
{
    static_cast<void>(next->storeChildBuffer(*processed));
}

size_t columnOf(const ArrowBufferRef& layout, const Record::RecordFieldIdentifier& name)
{
    const auto& fields = layout.getFields();
    const auto field = std::ranges::find(fields, name, &ArrowBufferRef::Field::name);
    INVARIANT(field != fields.end(), "The UDF batch has no column {}", name);
    return static_cast<size_t>(std::distance(fields.begin(), field));
}
}

UDFBatchPhysicalOperator::UDFBatchPhysicalOperator(
    std::vector<Argument> arguments,
    std::vector<Result> results,
    const std::vector<UdfDescriptor>& udfs,
    const UdfInterpreter interpreter,
    std::shared_ptr<ArrowBufferRef> batchLayout,
    std::vector<Record::RecordFieldIdentifier> outputFields,
    const bool fused)
    : arguments(std::move(arguments)), outputFields(std::move(outputFields)), batch(std::make_shared<Batch>())
{
    PRECONDITION(results.size() == udfs.size(), "A UDF operator needs one result per UDF");
    batch->stage = std::make_shared<UdfStage>(udfs, interpreter, fused);
    auto argument = this->arguments.begin();
    for (const auto& [udf, result] : std::views::zip(udfs, results))
    {
        auto& columns = batch->argumentColumns.emplace_back();
        for (size_t i = 0; i < udf.getArgTypes().size(); ++i, ++argument)
        {
            PRECONDITION(argument != this->arguments.end(), "A UDF operator needs the arguments of all its UDFs");
            columns.push_back(columnOf(*batchLayout, argument->field));
            batch->allArgumentColumns.push_back(columns.back());
        }
        batch->resultColumns.push_back(columnOf(*batchLayout, result.field));
    }
    batch->layout = std::move(batchLayout);
}

void UDFBatchPhysicalOperator::setup(ExecutionContext&, CompilationContext& compilationContext) const
{
    for (const auto& argument : arguments)
    {
        argument.function.setup(compilationContext);
    }
}

void UDFBatchPhysicalOperator::open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const
{
    openChild(ctx, recordBuffer);
    ctx.setLocalOperatorState(id, std::make_unique<UdfBatchState>(RecordBuffer{ctx.allocateBuffer()}));
}

void UDFBatchPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    auto* const state = dynamic_cast<UdfBatchState*>(ctx.getLocalState(id));
    Record batchRecord = record;
    for (const auto& argument : arguments)
    {
        batchRecord.write(argument.field, argument.function.execute(record, ctx.pipelineMemoryProvider.arena));
    }

    auto written = batch->layout->writeRecord(state->numberOfRecords, state->batch, batchRecord, ctx.pipelineMemoryProvider.bufferProvider);
    /// A full batch runs first; a new batch holds at least one record.
    if (!written.successful)
    {
        processBatch(ctx);
        RecordBuffer next{ctx.allocateBuffer()};
        nautilus::invoke(keepAlive, next.getReference(), state->batch.getReference());
        state->batch = next;
        state->numberOfRecords = uint64_t{0};
        written = batch->layout->writeRecord(state->numberOfRecords, state->batch, batchRecord, ctx.pipelineMemoryProvider.bufferProvider);
    }
    state->numberOfRecords = state->numberOfRecords + written.writtenRecords;
}

void UDFBatchPhysicalOperator::close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const
{
    const auto* const state = dynamic_cast<UdfBatchState*>(ctx.getLocalState(id));
    if (state->numberOfRecords > uint64_t{0})
    {
        processBatch(ctx);
    }
    closeChild(ctx, recordBuffer);
}

void UDFBatchPhysicalOperator::processBatch(ExecutionContext& ctx) const
{
    auto* const state = dynamic_cast<UdfBatchState*>(ctx.getLocalState(id));
    const auto bufferProvider = ctx.pipelineMemoryProvider.bufferProvider;
    batch->layout->finalize(state->batch, state->numberOfRecords, bufferProvider);
    nautilus::invoke(
        runUdfs,
        nautilus::val<const Batch*>(batch.get()),
        state->batch.getReference(),
        state->numberOfRecords,
        bufferProvider);
    for (nautilus::val<uint64_t> i = uint64_t{0}; i < state->numberOfRecords; i = i + uint64_t{1})
    {
        auto record = batch->layout->readRecord(outputFields, state->batch, i);
        executeChild(ctx, record);
    }
}

std::optional<PhysicalOperator> UDFBatchPhysicalOperator::getChild() const
{
    return child;
}

void UDFBatchPhysicalOperator::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}

}
