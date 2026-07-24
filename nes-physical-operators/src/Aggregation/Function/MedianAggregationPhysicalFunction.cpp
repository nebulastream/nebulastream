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
#include <Aggregation/Function/MedianNautilusFunctions.hpp>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <nautilus/function.hpp>

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
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

MedianAggregationPhysicalFunction::MedianAggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<PagedVectorTupleLayout> tupleLayout)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , tupleLayout(std::move(tupleLayout))
    /// The lowering rule lays the paged vector out as the single column being aggregated, so the name the value
    /// is stored under is the one and only field of that layout.
    , valueFieldName(this->tupleLayout->getSchema()[0].value().getFullyQualifiedName())
{
    PRECONDITION(
        std::ranges::size(this->tupleLayout->getSchema()) == 1,
        "Median stores a single value per row, but its layout has {} fields",
        std::ranges::size(this->tupleLayout->getSchema()));
}

void MedianAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState,
    PipelineMemoryProvider& pipelineMemoryProvider,
    CompilationContext&,
    const Record& record)
{
    const auto value = inputFunction.execute(record, pipelineMemoryProvider.arena);
    if (inputType.nullable)
    {
        /// SQL-standard: NULL inputs are not part of the median set, so skip writing them. Flip the null flag to
        /// false the first time we see a non-null value.
        if (not value.isNull())
        {
            storeNull(aggregationState, false);

            /// Skipping the first byte (null); the paged vector lives right after it.
            const auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>{1};
            PagedVectorRef pagedVectorRef(BorrowedNautilusBuffer::from(memArea), tupleLayout);
            pagedVectorRef.pushBack(record, pipelineMemoryProvider.bufferProvider);
        }
    }
    else
    {
        /// Adding the record to the paged vector. We are storing the full record in the paged vector for now.
        const auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState);
        PagedVectorRef pagedVectorRef(BorrowedNautilusBuffer::from(memArea), tupleLayout);
        pagedVectorRef.pushBack(record, pipelineMemoryProvider.bufferProvider);
    }
}

void MedianAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider& pipelineMemoryProvider,
    CompilationContext& compilationContext)
{
    using CombineSignature
        = void(nautilus::val<AggregationState*>, nautilus::val<AggregationState*>, nautilus::val<AbstractBufferProvider*>);
    auto& combineFunction = compilationContext.getOrRegisterNautilusFunction<CombineSignature>(
        fmt::format("Median_combine_{}", inputType.nullable ? "N" : "X"),
        [nullable = inputType.nullable](
            const nautilus::val<AggregationState*>& state1,
            const nautilus::val<AggregationState*>& state2,
            const nautilus::val<AbstractBufferProvider*>& bufferProvider)
        { medianCombineStates(state1, state2, bufferProvider, nullable); });
    combineFunction(aggregationState1, aggregationState2, pipelineMemoryProvider.bufferProvider);
}

Record MedianAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&, CompilationContext& compilationContext)
{
    /// SQL-standard: if no non-null value was observed, the null flag is still set and the paged vector is empty,
    /// so we return NULL. The early skip below also guards the numberOfEntries > 0 invariant.
    auto containsNull = nautilus::val<bool>{false};
    if (inputType.nullable)
    {
        containsNull = readNull(aggregationState);
    }

    /// The shared function can only hand back a plain value: VarVal's nullability is a C++-level property that
    /// would not survive the call, and assigning a non-nullable VarVal inside the traced branch below would drop
    /// the null flag of the other branch. So only the value crosses the branch, and the VarVal is built once
    /// afterwards with the traced null flag.
    nautilus::val<double> rawMedian = 0.0;
    if (!containsNull)
    {
        const auto pagedVectorBufferPtr = static_cast<nautilus::val<NES::TupleBuffer*>>(
            aggregationState + nautilus::val<uint64_t>{static_cast<uint64_t>(inputType.nullable)});
        /// The name must carry everything the generated code depends on, because a second median of the same name
        /// reuses this body and drops its own, captured layout. That is the stored column's type: page geometry is
        /// resolved at runtime, and the captured field name resolves to offset 0 of a single-column layout no
        /// matter which column it names.
        const auto& storedType = tupleLayout->getSchema()[0].value().getDataType();
        auto& lowerFunction = compilationContext.getOrRegisterNautilusFunction<nautilus::val<double>(nautilus::val<TupleBuffer*>)>(
            fmt::format("Median_lower_{}_{}", magic_enum::enum_name(storedType.type), storedType.nullable ? "N" : "X"),
            [layout = tupleLayout, field = valueFieldName](const nautilus::val<TupleBuffer*>& pagedVectorBuffer)
            { return medianOf(pagedVectorBuffer, layout, field); });
        rawMedian = lowerFunction(pagedVectorBufferPtr);
    }
    const VarVal medianValue = VarVal{rawMedian, true, containsNull}.castToType(resultType.type);

    /// Adding the median to the result record
    Record resultRecord;
    resultRecord.write(resultFieldIdentifier, medianValue);

    return resultRecord;
}

void MedianAggregationPhysicalFunction::reset(
    const nautilus::val<AggregationState*> aggregationState,
    PipelineMemoryProvider& pipelineMemoryProvider,
    CompilationContext& compilationContext)
{
    using ResetSignature = void(nautilus::val<AggregationState*>, nautilus::val<AbstractBufferProvider*>, nautilus::val<uint64_t>);
    auto& resetFunction = compilationContext.getOrRegisterNautilusFunction<ResetSignature>(
        fmt::format("Median_reset_{}", inputType.nullable ? "N" : "X"),
        [nullable = inputType.nullable](
            const nautilus::val<AggregationState*>& state,
            const nautilus::val<AbstractBufferProvider*>& bufferProvider,
            const nautilus::val<uint64_t>& tupleSize) { medianResetState(state, bufferProvider, tupleSize, nullable); });
    resetFunction(
        aggregationState, pipelineMemoryProvider.bufferProvider, nautilus::val<uint64_t>{tupleLayout->getSchema().getSizeInBytes()});
}

void MedianAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> aggregationState)
{
    invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {
            /// Calls the destructor of the PagedVector
            auto* pagedVectorBuffer
                = reinterpret_cast<TupleBuffer*>(pagedVectorMemArea); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
            pagedVectorBuffer->~TupleBuffer();
        },
        aggregationState + nautilus::val<uint64_t>{static_cast<uint64_t>(inputType.nullable)});
}

size_t MedianAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    /// ContainsNullValues (1B) + Pagedvector
    return static_cast<uint64_t>(inputType.nullable) + sizeof(TupleBuffer);
}

AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterMedianAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    INVARIANT(arguments.tupleLayout.has_value(), "Tuple layout paged vector not set");
    return std::make_shared<MedianAggregationPhysicalFunction>(
        std::move(arguments.inputType),
        std::move(arguments.resultType),
        arguments.inputFunction,
        arguments.resultFieldIdentifier,
        arguments.tupleLayout.value());
}

}
