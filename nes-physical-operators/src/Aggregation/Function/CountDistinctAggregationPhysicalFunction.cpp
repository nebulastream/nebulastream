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
#include <cstring>
#include <limits>
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

ChainedHashMapConfig createValueSetConfig(const DataType& valueType, const uint64_t numberOfBuckets, const uint64_t bufferSize)
{
    const auto schema
        = std::vector{QualifiedUnboundField{valueField, valueType}} | std::ranges::to<Schema<QualifiedUnboundField, Ordered>>();
    auto [fieldKeys, fieldValues] = ChainedEntryMemoryProvider::createFieldOffsets(schema, {valueField}, {});
    return ChainedHashMapConfig{
        .entrySize = sizeof(ChainedHashMapEntry) + valueType.getSizeInBytesWithNull(),
        /// The chains of the hash set live in one pooled buffer, so there are not more buckets than fit into it.
        .numberOfBuckets = ChainedHashMap::limitNumberOfBuckets(numberOfBuckets, bufferSize),
        .bloomFilterParams = std::nullopt,
        .fieldKeys = std::move(fieldKeys),
        .fieldValues = std::move(fieldValues),
        .hashFunction = std::make_shared<MurMur3HashFunction>()};
}

/// Layout of the aggregation state: the child-buffer index of the hash set (NO_VALUE_SET while there is none), the number of
/// inline values, the slots of the inline values, and one more slot in which the compiled code hands a new value to
/// addInlineValueProxy. Once the hash set exists, it holds all values and the inline ones are stale.
constexpr uint32_t NO_VALUE_SET = std::numeric_limits<uint32_t>::max();
constexpr uint64_t INLINE_COUNT_OFFSET = sizeof(uint32_t);
constexpr uint64_t INLINE_VALUES_OFFSET = 2 * sizeof(uint32_t);

nautilus::val<uint32_t*> valueSetIndexRef(const nautilus::val<AggregationState*>& aggregationState)
{
    return static_cast<nautilus::val<uint32_t*>>(static_cast<nautilus::val<int8_t*>>(aggregationState));
}

nautilus::val<uint32_t*> inlineCountRef(const nautilus::val<AggregationState*>& aggregationState)
{
    return static_cast<nautilus::val<uint32_t*>>(
        static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>{INLINE_COUNT_OFFSET});
}

nautilus::val<uint64_t> readInlineCount(const nautilus::val<AggregationState*>& aggregationState)
{
    const nautilus::val<uint32_t> numberOfInlineValues = *inlineCountRef(aggregationState);
    return static_cast<nautilus::val<uint64_t>>(numberOfInlineValues);
}

nautilus::val<int8_t*>
inlineValueRef(const nautilus::val<AggregationState*>& aggregationState, const nautilus::val<uint64_t>& index, const uint64_t valueSize)
{
    return static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>{INLINE_VALUES_OFFSET}
        + index * nautilus::val<uint64_t>{valueSize};
}

/// Adds the value in the slot behind the inline values to them, unless it is among them already. Values are compared bytewise. Returns
/// true if the value has to go to the hash set instead: the state has one already, or all inline slots are taken. Scanning the slots
/// here rather than in traced code keeps the compiled code of each COUNT(DISTINCT) small, which matters for query compilation time.
bool addInlineValueProxy(int8_t* aggregationState, const uint64_t valueSize, const uint64_t inlineCapacity)
{
    uint32_t valueSetIndex = 0;
    uint32_t numberOfInlineValues = 0;
    std::memcpy(&valueSetIndex, aggregationState, sizeof(uint32_t));
    std::memcpy(&numberOfInlineValues, aggregationState + INLINE_COUNT_OFFSET, sizeof(uint32_t));
    if (valueSetIndex != NO_VALUE_SET)
    {
        return true;
    }
    int8_t* const inlineValues = aggregationState + INLINE_VALUES_OFFSET;
    const int8_t* const newValue = inlineValues + (inlineCapacity * valueSize);
    for (uint32_t index = 0; index < numberOfInlineValues; ++index)
    {
        if (std::memcmp(inlineValues + (index * valueSize), newValue, valueSize) == 0)
        {
            return false;
        }
    }
    if (numberOfInlineValues == inlineCapacity)
    {
        return true;
    }
    std::memcpy(inlineValues + (numberOfInlineValues * valueSize), newValue, valueSize);
    ++numberOfInlineValues;
    std::memcpy(aggregationState + INLINE_COUNT_OFFSET, &numberOfInlineValues, sizeof(uint32_t));
    return false;
}

/// Adds the inline values of the source state to the target state, as addInlineValueProxy does with one value. Returns the index of
/// the first source value that has to go to the target's hash set; all later ones have to go there as well. The source must not have
/// a hash set.
uint64_t mergeInlineValuesProxy(int8_t* target, const int8_t* source, const uint64_t valueSize, const uint64_t inlineCapacity)
{
    uint32_t numberOfSourceValues = 0;
    std::memcpy(&numberOfSourceValues, source + INLINE_COUNT_OFFSET, sizeof(uint32_t));
    int8_t* const newValue = target + INLINE_VALUES_OFFSET + (inlineCapacity * valueSize);
    for (uint32_t index = 0; index < numberOfSourceValues; ++index)
    {
        std::memcpy(newValue, source + INLINE_VALUES_OFFSET + (index * valueSize), valueSize);
        if (addInlineValueProxy(target, valueSize, inlineCapacity))
        {
            return index;
        }
    }
    return numberOfSourceValues;
}

OwnedNautilusBuffer loadValueSet(const nautilus::val<AggregationState*>& aggregationState, BorrowedNautilusBuffer parentBuffer)
{
    OwnedNautilusBuffer valueSetBuffer;
    nautilus::invoke(
        +[](TupleBuffer* parent, TupleBuffer* out, const uint32_t* indexPtr) { *out = parent->loadChildBuffer(ChildBufferIndex{*indexPtr}); },
        parentBuffer.asArg(),
        valueSetBuffer.asArg(),
        valueSetIndexRef(aggregationState));
    return valueSetBuffer;
}

void insertIntoValueSet(
    const nautilus::val<AggregationState*>& aggregationState,
    BorrowedNautilusBuffer parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider,
    const ChainedHashMapConfig& valueSetConfig,
    const VarVal& value)
{
    auto valueSetBuffer = loadValueSet(aggregationState, parentBuffer);
    ChainedHashMapRef valueSet{BorrowedNautilusBuffer::from(valueSetBuffer.asArg()), valueSetConfig};
    Record valueRecord;
    valueRecord.write(valueField, value);
    valueSet.findOrCreateEntry(valueRecord, [](const nautilus::val<AbstractHashMapEntry*>&) { }, pipelineMemoryProvider.bufferProvider);
}
}

CountDistinctAggregationPhysicalFunction::CountDistinctAggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Record::RecordFieldIdentifier resultFieldIdentifier,
    const uint64_t numberOfBuckets,
    const uint64_t bufferSize,
    const uint64_t inlineValues)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , valueSetConfig(createValueSetConfig(this->inputType, numberOfBuckets, bufferSize))
    , inlineValueType(DataType{this->inputType.type, DataType::NULLABLE::NOT_NULLABLE})
    , inlineCapacity(this->inputType.isType(DataType::Type::VARSIZED) ? 0 : inlineValues)
{
}

void CountDistinctAggregationPhysicalFunction::createValueSet(
    const nautilus::val<AggregationState*>& aggregationState,
    BorrowedNautilusBuffer parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider) const
{
    *valueSetIndexRef(aggregationState) = nautilus::invoke(
        +[](TupleBuffer* parent,
            AbstractBufferProvider* bufferProvider IF_PRECONDITION(, const uint64_t entrySize),
            const uint64_t numberOfBuckets)
        {
            auto valueSetBuffer = bufferProvider->getBufferBlocking();
            ChainedHashMap::init(valueSetBuffer IF_PRECONDITION(, entrySize), numberOfBuckets, 0);
            return parent->storeChildBuffer(valueSetBuffer).getRawValue();
        },
        parentBuffer.asArg(),
        pipelineMemoryProvider.bufferProvider IF_PRECONDITION(, nautilus::val<uint64_t>{valueSetConfig.entrySize}),
        nautilus::val<uint64_t>{valueSetConfig.numberOfBuckets});

}

void CountDistinctAggregationPhysicalFunction::addValue(
    const nautilus::val<AggregationState*>& aggregationState,
    BorrowedNautilusBuffer parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider,
    const VarVal& value) const
{
    /// Without inline values (e.g. var-sized ones) the inline code must not be traced at all.
    if (inlineCapacity == 0)
    {
        if (*valueSetIndexRef(aggregationState) == nautilus::val<uint32_t>{NO_VALUE_SET})
        {
            createValueSet(aggregationState, parentBuffer, pipelineMemoryProvider);
        }
        insertIntoValueSet(aggregationState, parentBuffer, pipelineMemoryProvider, valueSetConfig, value);
        return;
    }

    const auto valueSize = inlineValueType.getSizeInBytesWithoutNull();
    const nautilus::val<uint64_t> newValueSlot{inlineCapacity};
    value.castToType(inlineValueType.type).writeToMemory(inlineValueRef(aggregationState, newValueSlot, valueSize));
    const nautilus::val<bool> toValueSet = nautilus::invoke(
        addInlineValueProxy,
        static_cast<nautilus::val<int8_t*>>(aggregationState),
        nautilus::val<uint64_t>{valueSize},
        nautilus::val<uint64_t>{inlineCapacity});
    if (toValueSet)
    {
        /// The value is in the slot behind the inline values. If the state has no hash set yet, all inline slots are taken, and the
        /// set gets the inline values and the new one, which are then contiguous. Either way, the set is filled by one insert, as
        /// every traced copy of the insert adds to the compilation time.
        nautilus::val<uint64_t> firstSlot = newValueSlot;
        if (*valueSetIndexRef(aggregationState) == nautilus::val<uint32_t>{NO_VALUE_SET})
        {
            createValueSet(aggregationState, parentBuffer, pipelineMemoryProvider);
            firstSlot = nautilus::val<uint64_t>{0};
        }
        for (nautilus::val<uint64_t> slot = firstSlot; slot <= newValueSlot; slot = slot + nautilus::val<uint64_t>{1})
        {
            const auto slotValue = VarVal::readNonNullableVarValFromMemory(inlineValueRef(aggregationState, slot, valueSize), inlineValueType);
            insertIntoValueSet(aggregationState, parentBuffer, pipelineMemoryProvider, valueSetConfig, slotValue);
        }
    }
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
        addValue(aggregationState, parentBuffer, pipelineMemoryProvider, value);
    }
}

void CountDistinctAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    BorrowedNautilusBuffer parentBuffer1,
    const nautilus::val<AggregationState*> aggregationState2,
    BorrowedNautilusBuffer parentBuffer2,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    const nautilus::val<bool> sourceHasValueSet = *valueSetIndexRef(aggregationState2) != nautilus::val<uint32_t>{NO_VALUE_SET};
    /// Without inline values (e.g. var-sized ones) the inline code must not be traced at all; a source without a hash set is empty then.
    if (inlineCapacity == 0)
    {
        if (sourceHasValueSet && *valueSetIndexRef(aggregationState1) == nautilus::val<uint32_t>{NO_VALUE_SET})
        {
            createValueSet(aggregationState1, parentBuffer1, pipelineMemoryProvider);
        }
    }
    else
    {
        /// Inline source values go to the target's inline values as far as they fit. The ones that do not fit, or all of them if the
        /// source has a hash set, need a hash set in the target. A target that gets one now moves its inline values into it. The
        /// values to insert are the target's inline values (if it gets a hash set now) and the remaining source values; one loop
        /// inserts both, as every traced copy of the insert adds to the compilation time.
        const auto valueSize = inlineValueType.getSizeInBytesWithoutNull();
        nautilus::val<uint64_t> firstSourceValue = readInlineCount(aggregationState2);
        nautilus::val<uint64_t> numberOfSourceValues = firstSourceValue;
        if (!sourceHasValueSet)
        {
            firstSourceValue = nautilus::invoke(
                mergeInlineValuesProxy,
                static_cast<nautilus::val<int8_t*>>(aggregationState1),
                static_cast<nautilus::val<int8_t*>>(aggregationState2),
                nautilus::val<uint64_t>{valueSize},
                nautilus::val<uint64_t>{inlineCapacity});
        }
        if (sourceHasValueSet || firstSourceValue < numberOfSourceValues)
        {
            nautilus::val<uint64_t> numberOfTargetValues{0};
            if (*valueSetIndexRef(aggregationState1) == nautilus::val<uint32_t>{NO_VALUE_SET})
            {
                createValueSet(aggregationState1, parentBuffer1, pipelineMemoryProvider);
                numberOfTargetValues = readInlineCount(aggregationState1);
            }
            nautilus::val<uint64_t> numberOfRemainingSourceValues{0};
            if (!sourceHasValueSet)
            {
                numberOfRemainingSourceValues = numberOfSourceValues - firstSourceValue;
            }
            for (nautilus::val<uint64_t> index = 0; index < numberOfTargetValues + numberOfRemainingSourceValues;
                 index = index + nautilus::val<uint64_t>{1})
            {
                nautilus::val<int8_t*> valueRef = inlineValueRef(aggregationState1, index, valueSize);
                if (index >= numberOfTargetValues)
                {
                    valueRef = inlineValueRef(aggregationState2, firstSourceValue + index - numberOfTargetValues, valueSize);
                }
                const auto value = VarVal::readNonNullableVarValFromMemory(valueRef, inlineValueType);
                insertIntoValueSet(aggregationState1, parentBuffer1, pipelineMemoryProvider, valueSetConfig, value);
            }
        }
    }
    if (sourceHasValueSet)
    {
        /// The target has a hash set now: insert the source's entries into it.
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
}

Record CountDistinctAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState,
    BorrowedNautilusBuffer parentBuffer,
    PipelineMemoryProvider& /*pipelineMemoryProvider*/)
{
    nautilus::val<uint64_t> distinctValues = readInlineCount(aggregationState);
    if (*valueSetIndexRef(aggregationState) != nautilus::val<uint32_t>{NO_VALUE_SET})
    {
        auto valueSetBuffer = loadValueSet(aggregationState, parentBuffer);
        distinctValues = nautilus::invoke(
            +[](const TupleBuffer* valueSet) { return ChainedHashMap::load(*valueSet).getTotalNumberOfRecords(); }, valueSetBuffer.asArg());
    }

    Record resultRecord;
    resultRecord.write(resultFieldIdentifier, VarVal{distinctValues}.castToType(resultType.type));
    return resultRecord;
}

void CountDistinctAggregationPhysicalFunction::reset(
    const nautilus::val<AggregationState*> aggregationState,
    BorrowedNautilusBuffer /*parentBuffer*/,
    PipelineMemoryProvider& /*pipelineMemoryProvider*/)
{
    /// No hash set yet: it is created only once the inline values do not suffice.
    *valueSetIndexRef(aggregationState) = nautilus::val<uint32_t>{NO_VALUE_SET};
    *inlineCountRef(aggregationState) = nautilus::val<uint32_t>{0};
}

void CountDistinctAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> /*aggregationState*/)
{
    /// The hash set is a child buffer of the parent hash map buffer and is released with it.
}

size_t CountDistinctAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    /// One slot more than inline values, for handing a new value to addInlineValueProxy.
    const auto numberOfSlots = inlineCapacity == 0 ? 0 : inlineCapacity + 1;
    return INLINE_VALUES_OFFSET + (numberOfSlots * inlineValueType.getSizeInBytesWithoutNull());
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
        arguments.bufferSize,
        arguments.countDistinctInlineValues);
}

}
