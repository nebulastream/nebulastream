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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <nautilus/nautilus_function.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// Median aggregation.
///
/// The lift, combine, lower and reset bodies are extracted into dedicated nautilus functions
/// (NautilusFunction, i.e. Pattern 2 cross-calls) so that they are compiled once and called from the
/// pipeline rather than inlined at every aggregation. The functions live in a global, statically allocated,
/// templated table (see the .cpp): one set per (input type, result type, nullability) with clear, type-derived
/// names. As nautilus deduplicates functions by name, all medians of the same type in a pipeline share a
/// single compiled instantiation. This object merely selects the matching set and forwards to it.
///
/// To keep the bodies type-parametric, the per-column work stays in the thin virtual wrappers: the wrapper
/// computes the input value via inputFunction and hands it to the nautilus function through a scratch slot,
/// and it reads the lowered result back from a scratch slot before writing it under the column-specific
/// result field. Consequently the paged vector stores only the single aggregated value per row.
class MedianAggregationPhysicalFunction : public AggregationPhysicalFunction
{
public:
    /// Uniform signatures of the extracted nautilus functions. The concrete data type lives inside the body
    /// (selected by template parameters), so these handle types are independent of it; values cross the
    /// boundary through scratch memory (val<int8_t*>) because a Record/VarVal cannot.
    using LiftFunction = nautilus::NautilusFunction<std::function<void(
        nautilus::val<AggregationState*>,
        nautilus::val<int8_t*>,
        nautilus::val<bool>,
        nautilus::val<AbstractBufferProvider*>)>>;
    using CombineFunction
        = nautilus::NautilusFunction<std::function<void(nautilus::val<AggregationState*>, nautilus::val<AggregationState*>)>>;
    using LowerFunction = nautilus::NautilusFunction<
        std::function<void(nautilus::val<AggregationState*>, nautilus::val<int8_t*>, nautilus::val<int8_t*>)>>;
    using ResetFunction = nautilus::NautilusFunction<std::function<void(nautilus::val<AggregationState*>)>>;

    /// The four extracted nautilus functions selected for this median's data types. Non-owning pointers into
    /// the global static function table (see the .cpp).
    struct MedianFunctions
    {
        LiftFunction* lift = nullptr;
        CombineFunction* combine = nullptr;
        LowerFunction* lower = nullptr;
        ResetFunction* reset = nullptr;
    };

    MedianAggregationPhysicalFunction(
        DataType inputType,
        DataType resultType,
        PhysicalFunction inputFunction,
        Record::RecordFieldIdentifier resultFieldIdentifier,
        std::shared_ptr<TupleBufferRef> bufferRefPagedVector);
    void lift(
        const nautilus::val<AggregationState*>& aggregationState,
        PipelineMemoryProvider& pipelineMemoryProvider,
        const Record& record) override;
    void combine(
        nautilus::val<AggregationState*> aggregationState1,
        nautilus::val<AggregationState*> aggregationState2,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    Record lower(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider) override;
    void reset(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider) override;
    void cleanup(nautilus::val<AggregationState*> aggregationState) override;
    [[nodiscard]] size_t getSizeOfStateInBytes() const override;
    ~MedianAggregationPhysicalFunction() override = default;

private:
    /// Selected from the global static function table by (input type, result type, nullability) in the constructor.
    MedianFunctions functions;
};

}
