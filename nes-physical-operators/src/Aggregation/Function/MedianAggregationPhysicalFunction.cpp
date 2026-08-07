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

#include <Aggregation/Function/MedianAggregationPhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Interface/BTree/BTree.hpp>
#include <Interface/BTree/BTreeRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <nautilus/function.hpp>

#include <DataTypes/SchemaFwd.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <nautilus/std/cstring.h>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <CompilationContext.hpp>
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
const auto& getMedianValueFieldIdentifier()
{
    static const auto Identifier = QualifiedIdentifier::parse("__median_value");
    return Identifier;
}

std::shared_ptr<BTreeTupleLayout> createMedianTupleLayout(const DataType& inputType)
{
    return std::make_shared<DefaultBTreeTupleLayout>(
        Schema<QualifiedUnboundField, Ordered>{QualifiedUnboundField{getMedianValueFieldIdentifier(), inputType}});
}

BTreeComparator::RecordComparator getMedianComparator()
{
    return [](const Record& lhs, const Record& rhs) -> nautilus::val<bool>
    {
        return (lhs.read(getMedianValueFieldIdentifier()) < rhs.read(getMedianValueFieldIdentifier())).getRawValueAs<nautilus::val<bool>>();
    };
}
}

MedianAggregationPhysicalFunction::MedianAggregationPhysicalFunction(
    DataType inputType, DataType resultType, PhysicalFunction inputFunction, Record::RecordFieldIdentifier resultFieldIdentifier)
    : AggregationPhysicalFunction(inputType, std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , tupleLayout(createMedianTupleLayout(inputType))
{
}

void MedianAggregationPhysicalFunction::setup(CompilationContext& compilationContext)
{
    auto comparator = std::make_unique<BTreeComparator>(
        compilationContext, tupleLayout, fmt::format("medianComparator:{}", fmt::streamed(inputType)), getMedianComparator());
    activeComparator = comparator.get();
    comparators.emplace_back(std::move(comparator));
}

void MedianAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState,
    nautilus::val<TupleBuffer*> parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider,
    const Record& record)
{
    PRECONDITION(activeComparator != nullptr, "Median BTree comparator must be set up before lift");
    const auto value = inputFunction.execute(record, pipelineMemoryProvider.arena);
    Record medianValueRecord;
    medianValueRecord.write(getMedianValueFieldIdentifier(), value);
    if (inputType.nullable)
    {
        /// SQL-standard: NULL inputs are not part of the median set, so skip writing them. Flip the null flag to
        /// false the first time we see a non-null value.
        if (not value.isNull())
        {
            storeNull(aggregationState, false);

            /// Skipping the first byte (null); the BTree child index lives right after it.
            const auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState + nautilus::val<uint64_t>{1});
            OwnedNautilusBuffer treeBuffer;
            nautilus::invoke(
                +[](TupleBuffer* parent, TupleBuffer* out, const uint32_t* indexPtr)
                { *out = parent->loadChildBuffer(ChildBufferIndex{*indexPtr}); },
                parentBuffer,
                treeBuffer.asArg(),
                static_cast<nautilus::val<uint32_t*>>(memArea));

            const BTreeRef tree{BorrowedNautilusBuffer::from(treeBuffer.asArg()), tupleLayout};
            tree.append(medianValueRecord, pipelineMemoryProvider.bufferProvider, *activeComparator);
        }
    }
    else
    {
        /// Load the BTree buffer from the parent via the stored child index.
        const auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState);
        OwnedNautilusBuffer treeBuffer;
        nautilus::invoke(
            +[](TupleBuffer* parent, TupleBuffer* out, const uint32_t* indexPtr)
            { *out = parent->loadChildBuffer(ChildBufferIndex{*indexPtr}); },
            parentBuffer,
            treeBuffer.asArg(),
            static_cast<nautilus::val<uint32_t*>>(memArea));

        const BTreeRef tree{BorrowedNautilusBuffer::from(treeBuffer.asArg()), tupleLayout};
        tree.append(medianValueRecord, pipelineMemoryProvider.bufferProvider, *activeComparator);
    }
}

void MedianAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    nautilus::val<TupleBuffer*> parentBuffer1,
    const nautilus::val<AggregationState*> aggregationState2,
    nautilus::val<TupleBuffer*> parentBuffer2,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    PRECONDITION(activeComparator != nullptr, "Median BTree comparator must be set up before combine");
    auto memArea1 = static_cast<nautilus::val<int8_t*>>(aggregationState1);
    auto memArea2 = static_cast<nautilus::val<int8_t*>>(aggregationState2);

    if (inputType.nullable)
    {
        /// Combining the null values
        const auto containsNull1 = readNull(aggregationState1);
        const auto containsNull2 = readNull(aggregationState2);
        storeNull(aggregationState1, containsNull1 and containsNull2);

        /// Skipping the first byte (null)
        memArea1 += nautilus::val<uint64_t>{1};
        memArea2 += nautilus::val<uint64_t>{1};
    }

    OwnedNautilusBuffer treeBuffer1;
    OwnedNautilusBuffer treeBuffer2;
    nautilus::invoke(
        +[](TupleBuffer* parent1,
            TupleBuffer* output1,
            const uint32_t* indexPtr1,
            TupleBuffer* parent2,
            TupleBuffer* output2,
            const uint32_t* indexPtr2) -> void
        {
            *output1 = parent1->loadChildBuffer(ChildBufferIndex{*indexPtr1});
            *output2 = parent2->loadChildBuffer(ChildBufferIndex{*indexPtr2});
        },
        parentBuffer1,
        treeBuffer1.asArg(),
        static_cast<nautilus::val<uint32_t*>>(memArea1),
        parentBuffer2,
        treeBuffer2.asArg(),
        static_cast<nautilus::val<uint32_t*>>(memArea2));

    const BTreeRef destination{BorrowedNautilusBuffer::from(treeBuffer1.asArg()), tupleLayout};
    const BTreeRef source{BorrowedNautilusBuffer::from(treeBuffer2.asArg()), tupleLayout};
    const auto sourceSize = source.size();
    for (nautilus::val<uint64_t> index = 0; index < sourceSize; index = index + 1)
    {
        destination.append(source.at(index), pipelineMemoryProvider.bufferProvider, *activeComparator);
    }
}

Record MedianAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState,
    nautilus::val<TupleBuffer*> parentBuffer,
    PipelineMemoryProvider& /*pipelineMemoryProvider*/)
{
    /// If it contains null values, we simply return a null value
    auto containsNull = nautilus::val<bool>{false};
    if (inputType.nullable)
    {
        containsNull = readNull(aggregationState);
    }

    const VarVal zero{nautilus::val<uint64_t>(0), true, true};
    VarVal medianValue = zero.castToType(resultType.type);

    if (!containsNull)
    {
        /// Load the BTree buffer from the parent via its stored child index.
        auto memArea
            = static_cast<nautilus::val<int8_t*>>(aggregationState + nautilus::val<uint64_t>{static_cast<uint64_t>(inputType.nullable)});
        OwnedNautilusBuffer treeBuffer;
        nautilus::invoke(
            +[](TupleBuffer* parent, TupleBuffer* out, const uint32_t* indexPtr)
            { *out = parent->loadChildBuffer(ChildBufferIndex{*indexPtr}); },
            parentBuffer,
            treeBuffer.asArg(),
            static_cast<nautilus::val<uint32_t*>>(memArea));

        const BTreeRef tree{BorrowedNautilusBuffer::from(treeBuffer.asArg()), tupleLayout};
        const auto numberOfEntries = tree.size();

        const auto medianPos1 = (numberOfEntries - 1) / 2;
        const auto medianPos2 = numberOfEntries / 2;
        const auto medianRecord1 = tree.at(medianPos1);
        const auto medianRecord2 = tree.at(medianPos2);

        /// Regardless of cardinality, median is the average of the two middle positions. For odd cardinalities,
        /// both positions refer to the same record.
        const auto& medianValue1 = medianRecord1.read(getMedianValueFieldIdentifier());
        const auto& medianValue2 = medianRecord2.read(getMedianValueFieldIdentifier());
        const VarVal two = nautilus::val<uint64_t>(2);
        medianValue
            = (medianValue1.castToType(resultType.type) + medianValue2.castToType(resultType.type)) / two.castToType(resultType.type);
    }


    /// Adding the median to the result record
    Record resultRecord;
    resultRecord.write(resultFieldIdentifier, medianValue);

    return resultRecord;
}

void MedianAggregationPhysicalFunction::reset(
    const nautilus::val<AggregationState*> aggregationState,
    nautilus::val<TupleBuffer*> parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    const nautilus::val<uint64_t> tupleSize = inputType.getSizeInBytesWithNull();
    const nautilus::val<uint32_t> childBufferIndexVal = nautilus::invoke(
        +[](TupleBuffer* parentBuffer, AbstractBufferProvider* bufferProvider, uint64_t tupleSize)
        {
            /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): aggregation state stores a TupleBuffer at this slot.
            if (auto treeBuffer = bufferProvider->getUnpooledBuffer(BTree::getMainBufferSize()))
            {
                BTree::init(*treeBuffer, bufferProvider->getBufferSize(), tupleSize);
                auto childBufferIndex = parentBuffer->storeChildBuffer(*treeBuffer);
                return childBufferIndex.getRawValue();
            }
            throw BufferAllocationFailure("No unpooled TupleBuffer available for median aggregation BTree!");
        },
        parentBuffer,
        pipelineMemoryProvider.bufferProvider,
        tupleSize);

    auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState);
    if (inputType.nullable)
    {
        /// Initialize the null flag to "no value seen yet" so all-NULL windows correctly emit NULL
        storeNull(aggregationState, true);
        /// Skipping the first byte (null); the BTree child index lives right after it.
        memArea += nautilus::val<uint64_t>{1};
    }
    auto indexMemArea = static_cast<nautilus::val<uint32_t*>>(memArea);
    *indexMemArea = childBufferIndexVal;
}

void MedianAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> /*aggregationState*/)
{
    /// No-op: the BTree buffer is stored as a child of the parent hash map TupleBuffer and
    /// is released automatically when the parent is released.
}

size_t MedianAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    /// ContainsNullValues (1B, optional) + uint32_t child buffer index (4B)
    return static_cast<uint64_t>(inputType.nullable) + sizeof(uint32_t);
}

AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterMedianAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    return std::make_shared<MedianAggregationPhysicalFunction>(
        std::move(arguments.inputType), std::move(arguments.resultType), arguments.inputFunction, arguments.resultFieldIdentifier);
}

}
