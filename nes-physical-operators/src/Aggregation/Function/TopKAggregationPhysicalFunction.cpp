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

#include <Aggregation/Function/TopKAggregationPhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <ranges>
#include <utility>
#include <vector>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/ConstantValuePhysicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/BTree/BTreeRef.hpp>
#include <Interface/RowHeap/RowHeap.hpp>
#include <Interface/RowHeap/RowHeapRef.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <CompilationContext.hpp>
#include <nautilus/function.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <select.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{
Schema<QualifiedUnboundField, Ordered> createRowSchema(const Schema<QualifiedUnboundField, Ordered>& inputSchema)
{
    std::vector<QualifiedUnboundField> fields = inputSchema | std::ranges::to<std::vector>();
    for (const auto& arrivalField : TopKAggregationPhysicalFunction::arrivalFields())
    {
        fields.emplace_back(arrivalField, DataTypeProvider::provideDataType(DataType::Type::UINT64));
    }
    return fields | std::ranges::to<Schema<QualifiedUnboundField, Ordered>>();
}
}

std::vector<Record::RecordFieldIdentifier> TopKAggregationPhysicalFunction::arrivalFields()
{
    return {
        Record::RecordFieldIdentifier{Identifier::parse("$TOPKSEQUENCE")},
        Record::RecordFieldIdentifier{Identifier::parse("$TOPKCHUNK")},
        Record::RecordFieldIdentifier{Identifier::parse("$TOPKPOSITION")}};
}

TopKAggregationPhysicalFunction::TopKAggregationPhysicalFunction(
    const uint64_t limit,
    std::vector<SortKey> sortKeys,
    const Schema<QualifiedUnboundField, Ordered>& inputSchema,
    const uint64_t pageSize,
    std::string comparatorKey)
    : AggregationPhysicalFunction(
          DataTypeProvider::provideDataType(DataType::Type::UINT64),
          DataTypeProvider::provideDataType(DataType::Type::UINT64),
          ConstantUInt64ValueFunction{0},
          Record::RecordFieldIdentifier{Identifier::parse("$TOPK")})
    , limit(limit)
    , orderKeys(std::move(sortKeys))
    , rowLayout(std::make_shared<DefaultBTreeTupleLayout>(createRowSchema(inputSchema)))
    , rowSize(rowLayout->getSizeInBytes())
    , varSizedFieldOffsets(RowHeapRef::getVarSizedFieldOffsets(createRowSchema(inputSchema)))
    , pageSize(pageSize)
    , comparatorKey(std::move(comparatorKey))
{
    for (auto& arrivalField : arrivalFields())
    {
        orderKeys.push_back(SortKey{.field = std::move(arrivalField), .descending = false});
    }
}

OwnedNautilusBuffer
TopKAggregationPhysicalFunction::loadRows(const nautilus::val<AggregationState*>& aggregationState, BorrowedNautilusBuffer parentBuffer)
{
    OwnedNautilusBuffer rowsBuffer;
    nautilus::invoke(
        +[](TupleBuffer* parent, TupleBuffer* out, const uint32_t* indexPtr) { *out = parent->loadChildBuffer(ChildBufferIndex{*indexPtr}); },
        parentBuffer.asArg(),
        rowsBuffer.asArg(),
        static_cast<nautilus::val<uint32_t*>>(static_cast<nautilus::val<int8_t*>>(aggregationState)));
    return rowsBuffer;
}

nautilus::val<bool> TopKAggregationPhysicalFunction::comesBefore(const Record& first, const Record& second) const
{
    /// 1 if the first row comes before the second, -1 if after, 0 while all keys compared so far are equal.
    nautilus::val<int8_t> order = 0;
    for (const auto& key : nautilus::static_iterable(orderKeys))
    {
        const auto firstValue = first.read(key.field);
        const auto secondValue = second.read(key.field);
        const auto less = (firstValue < secondValue).getRawValueAs<nautilus::val<bool>>();
        const auto greater = (firstValue > secondValue).getRawValueAs<nautilus::val<bool>>();
        /// The direction is known at query compilation time, so it picks the comparison on the host.
        const auto& firstWins = key.descending ? greater : less;
        const auto& secondWins = key.descending ? less : greater;
        const auto undecided = order == nautilus::val<int8_t>{0};
        order = nautilus::select(
            undecided and firstWins,
            nautilus::val<int8_t>{1},
            nautilus::select(undecided and secondWins, nautilus::val<int8_t>{-1}, order));
    }
    return order == nautilus::val<int8_t>{1};
}

void TopKAggregationPhysicalFunction::setup(CompilationContext& compilationContext)
{
    auto comparator = std::make_shared<BTreeComparator>(
        compilationContext,
        rowLayout,
        comparatorKey,
        [this](const Record& first, const Record& second) { return comesBefore(first, second); });
    activeComparator = comparator.get();
    comparators.emplace_back(std::move(comparator));
}

const BTreeComparator& TopKAggregationPhysicalFunction::getComparator() const
{
    INVARIANT(activeComparator != nullptr, "The comparator of the top-k tree is compiled in the setup of its operator");
    return *activeComparator;
}

void TopKAggregationPhysicalFunction::insert(
    const RowHeapRef& rows, const Record& row, const nautilus::val<AbstractBufferProvider*>& bufferProvider) const
{
    if (rows.size() < nautilus::val<uint64_t>{limit})
    {
        rows.push(row, bufferProvider, getComparator());
    }
    else
    {
        /// The root is the last of the top rows.
        if (comesBefore(row, rows.at(0)))
        {
            rows.replaceTop(row, bufferProvider, getComparator());
        }
    }
}

void TopKAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState,
    BorrowedNautilusBuffer parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider,
    const Record& record)
{
    auto rowsBuffer = loadRows(aggregationState, parentBuffer);
    const RowHeapRef rows(BorrowedNautilusBuffer::from(rowsBuffer.asArg()), rowLayout);
    insert(rows, record, pipelineMemoryProvider.bufferProvider);
}

void TopKAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    BorrowedNautilusBuffer parentBuffer1,
    const nautilus::val<AggregationState*> aggregationState2,
    BorrowedNautilusBuffer parentBuffer2,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    auto targetBuffer = loadRows(aggregationState1, parentBuffer1);
    const RowHeapRef target(BorrowedNautilusBuffer::from(targetBuffer.asArg()), rowLayout);
    auto sourceBuffer = loadRows(aggregationState2, parentBuffer2);
    const RowHeapRef source(BorrowedNautilusBuffer::from(sourceBuffer.asArg()), rowLayout);
    const auto sourceSize = source.size();
    for (nautilus::val<uint64_t> index = 0; index < sourceSize; ++index)
    {
        insert(target, source.at(index), pipelineMemoryProvider.bufferProvider);
    }
}

Record TopKAggregationPhysicalFunction::lower(nautilus::val<AggregationState*>, BorrowedNautilusBuffer, PipelineMemoryProvider&)
{
    INVARIANT(false, "The top-k state holds rows and is read by its probe, not lowered into one record");
    return Record{};
}

void TopKAggregationPhysicalFunction::reset(
    const nautilus::val<AggregationState*> aggregationState,
    BorrowedNautilusBuffer parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    const nautilus::val<uint32_t> childBufferIndex = nautilus::invoke(
        +[](TupleBuffer* parent,
            AbstractBufferProvider* bufferProvider,
            const uint64_t limit,
            const uint64_t rowSize,
            const uint64_t pageSize,
            const std::vector<uint64_t>* varSizedFieldOffsets)
        {
            auto rowsBuffer = bufferProvider->getUnpooledBuffer(RowHeap::getMainBufferSize(limit, rowSize));
            if (not rowsBuffer.has_value())
            {
                throw BufferAllocationFailure("No unpooled TupleBuffer available for the rows of a top-k state");
            }
            RowHeap::init(rowsBuffer.value(), *bufferProvider, limit, rowSize, pageSize, *varSizedFieldOffsets);
            return parent->storeChildBuffer(rowsBuffer.value()).getRawValue();
        },
        parentBuffer.asArg(),
        pipelineMemoryProvider.bufferProvider,
        nautilus::val<uint64_t>{limit},
        nautilus::val<uint64_t>{rowSize},
        nautilus::val<uint64_t>{pageSize},
        nautilus::val<const std::vector<uint64_t>*>{&varSizedFieldOffsets});
    *static_cast<nautilus::val<uint32_t*>>(static_cast<nautilus::val<int8_t*>>(aggregationState)) = childBufferIndex;
}

void TopKAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*>)
{
    /// The heap is a child buffer of the parent hash map buffer and is released with it.
}

size_t TopKAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    return sizeof(uint32_t);
}

}
