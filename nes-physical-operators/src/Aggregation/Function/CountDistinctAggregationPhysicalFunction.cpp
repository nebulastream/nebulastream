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

#include <Aggregation/Function/CountDistinctAggregationPhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <ranges>
#include <utility>
#include <vector>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <DataTypes/VarVal.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Interface/Hash/MurMur3HashFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <nautilus/function.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace
{
/// The only key of the hash set.
const Record::RecordFieldIdentifier valueField{Identifier::parse("countDistinctValue")};

ChainedHashMapConfig createValueSetConfig(const DataType& valueType, const uint64_t numberOfBuckets, const uint64_t pageSize)
{
    const auto schema
        = std::vector{QualifiedUnboundField{valueField, valueType}} | std::ranges::to<Schema<QualifiedUnboundField, Ordered>>();
    auto [fieldKeys, fieldValues] = ChainedEntryMemoryProvider::createFieldOffsets(schema, {valueField}, {});
    return ChainedHashMapConfig{
        .entrySize = sizeof(ChainedHashMapEntry) + valueType.getSizeInBytesWithNull(),
        .numberOfBuckets = numberOfBuckets,
        .pageSize = pageSize,
        .bloomFilterParams = std::nullopt,
        .fieldKeys = std::move(fieldKeys),
        .fieldValues = std::move(fieldValues),
        .hashFunction = std::make_shared<MurMur3HashFunction>()};
}

/// The aggregation state holds only the child-buffer index of the hash set.
OwnedNautilusBuffer loadValueSet(const nautilus::val<AggregationState*>& aggregationState, BorrowedNautilusBuffer parentBuffer)
{
    OwnedNautilusBuffer valueSetBuffer;
    nautilus::invoke(
        +[](TupleBuffer* parent, TupleBuffer* out, const uint32_t* indexPtr) { *out = parent->loadChildBuffer(ChildBufferIndex{*indexPtr}); },
        parentBuffer.asArg(),
        valueSetBuffer.asArg(),
        static_cast<nautilus::val<uint32_t*>>(static_cast<nautilus::val<int8_t*>>(aggregationState)));
    return valueSetBuffer;
}
}

CountDistinctAggregationPhysicalFunction::CountDistinctAggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Record::RecordFieldIdentifier resultFieldIdentifier,
    const uint64_t numberOfBuckets,
    const uint64_t pageSize)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , valueSetConfig(createValueSetConfig(this->inputType, numberOfBuckets, pageSize))
{
}

void CountDistinctAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState,
    BorrowedNautilusBuffer parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider,
    const Record& record)
{
    /// SQL-standard: NULL is not a value, so COUNT(DISTINCT x) ignores records whose x is NULL.
    const auto value = inputFunction.execute(record, pipelineMemoryProvider.arena);
    if (not value.isNull())
    {
        auto valueSetBuffer = loadValueSet(aggregationState, parentBuffer);
        ChainedHashMapRef valueSet{BorrowedNautilusBuffer::from(valueSetBuffer.asArg()), valueSetConfig};
        Record valueRecord;
        valueRecord.write(valueField, value);
        valueSet.findOrCreateEntry(valueRecord, [](const nautilus::val<AbstractHashMapEntry*>&) { }, pipelineMemoryProvider.bufferProvider);
    }
}

void CountDistinctAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    BorrowedNautilusBuffer parentBuffer1,
    const nautilus::val<AggregationState*> aggregationState2,
    BorrowedNautilusBuffer parentBuffer2,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    auto targetBuffer = loadValueSet(aggregationState1, parentBuffer1);
    ChainedHashMapRef target{BorrowedNautilusBuffer::from(targetBuffer.asArg()), valueSetConfig};
    auto sourceBuffer = loadValueSet(aggregationState2, parentBuffer2);
    const auto sourceRef = BorrowedNautilusBuffer::from(sourceBuffer.asArg());
    const ChainedHashMapRef source{sourceRef, valueSetConfig};
    for (const auto entry : source)
    {
        const ChainedHashMapRef::ChainedEntryRef entryRef{entry, sourceRef, valueSetConfig.fieldKeys, valueSetConfig.fieldValues};
        /// A value the target has already needs no update, and a new one needs nothing but its key, which the insert copies.
        target.insertOrUpdateEntry(entryRef.entryRef, {}, {}, pipelineMemoryProvider.bufferProvider);
    }
}

Record CountDistinctAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState,
    BorrowedNautilusBuffer parentBuffer,
    PipelineMemoryProvider& /*pipelineMemoryProvider*/)
{
    auto valueSetBuffer = loadValueSet(aggregationState, parentBuffer);
    const nautilus::val<uint64_t> distinctValues = nautilus::invoke(
        +[](const TupleBuffer* valueSet) { return ChainedHashMap::load(*valueSet).getTotalNumberOfRecords(); }, valueSetBuffer.asArg());

    Record resultRecord;
    resultRecord.write(resultFieldIdentifier, VarVal{distinctValues}.castToType(resultType.type));
    return resultRecord;
}

void CountDistinctAggregationPhysicalFunction::reset(
    const nautilus::val<AggregationState*> aggregationState,
    BorrowedNautilusBuffer parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    const nautilus::val<uint32_t> childBufferIndex = nautilus::invoke(
        +[](TupleBuffer* parent,
            AbstractBufferProvider* bufferProvider IF_PRECONDITION(, const uint64_t entrySize),
            const uint64_t numberOfBuckets IF_PRECONDITION(, const uint64_t pageSize))
        {
            const auto bufferSize = ChainedHashMap::calculateBufferSize(numberOfBuckets, 0);
            if (auto valueSetBuffer = bufferProvider->getUnpooledBuffer(bufferSize))
            {
                ChainedHashMap::init(valueSetBuffer.value() IF_PRECONDITION(, entrySize), numberOfBuckets IF_PRECONDITION(, pageSize), 0);
                return parent->storeChildBuffer(valueSetBuffer.value()).getRawValue();
            }
            throw BufferAllocationFailure("No unpooled TupleBuffer of {}B available for the COUNT(DISTINCT) hash set", bufferSize);
        },
        parentBuffer.asArg(),
        pipelineMemoryProvider.bufferProvider IF_PRECONDITION(, nautilus::val<uint64_t>{valueSetConfig.entrySize}),
        nautilus::val<uint64_t>{valueSetConfig.numberOfBuckets} IF_PRECONDITION(, nautilus::val<uint64_t>{valueSetConfig.pageSize}));
    *static_cast<nautilus::val<uint32_t*>>(static_cast<nautilus::val<int8_t*>>(aggregationState)) = childBufferIndex;
}

void CountDistinctAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> /*aggregationState*/)
{
    /// The hash set is a child buffer of the parent hash map buffer and is released with it.
}

size_t CountDistinctAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    return sizeof(uint32_t);
}

AggregationPhysicalFunctionRegistryReturnType
CountDistinctAggregationPhysicalFunction::create(AggregationPhysicalFunctionRegistryArguments arguments)
{
    return std::make_shared<CountDistinctAggregationPhysicalFunction>(
        std::move(arguments.inputType),
        std::move(arguments.resultType),
        arguments.inputFunction,
        arguments.resultFieldIdentifier,
        arguments.countDistinctNumberOfBuckets,
        arguments.pageSize);
}

}
