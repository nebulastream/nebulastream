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

#include <memory>
#include <optional>
#include <vector>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <SliceStore/SliceStoreRef.hpp>
#include <Watermark/TimeFunction.hpp>
#include <CompilationContext.hpp>
#include <WindowBuildPhysicalOperator.hpp>

namespace NES
{
struct AggregationEmitSession;
struct AggregationAbsorbSession;
struct AggregationEmitRuntime;
struct AggregationAbsorbRuntime;

class AggregationBuildPhysicalOperator final : public WindowBuildPhysicalOperator
{
public:
    struct DonorLayout
    {
        size_t entrySize;
        std::vector<std::shared_ptr<AggregationPhysicalFunction>> functions;
    };

    AggregationBuildPhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        std::unique_ptr<TimeFunction> timeFunction,
        std::unique_ptr<SliceStoreRef> sliceStoreRef,
        std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationFunctions,
        ChainedHashMapConfig hashMapConfig,
        std::vector<PhysicalFunction> keyFunctions,
        std::optional<DonorLayout> donorLayout = std::nullopt);
    void setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;
    void emit(PipelineStateBuilder& state, PipelineExecutionContext& context) const override;
    void absorb(PipelineStateReader& state, PipelineExecutionContext& context) const override;
    void lowerEmit(nautilus::val<PipelineStateBuilder*> state, nautilus::val<PipelineExecutionContext*> context) const override;
    void lowerAbsorb(nautilus::val<PipelineStateReader*> state, nautilus::val<PipelineExecutionContext*> context) const override;
    /// Register the compiled state codec. Kept separate from setup so a codec harness can compile it without an execute pipeline.
    void registerMigrationFunctions(CompilationContext& compilationContext) const;

private:
    void emitCompiled(PipelineStateBuilder& state, PipelineExecutionContext& context) const;
    void absorbCompiled(PipelineStateReader& state, PipelineExecutionContext& context) const;
    void traceEmitState(
        nautilus::val<TupleBuffer*> output, nautilus::val<const AggregationEmitSession*> session, nautilus::val<uint64_t*> usedBytes) const;
    void traceAbsorbState(nautilus::val<const TupleBuffer*> input, nautilus::val<AggregationAbsorbSession*> session) const;
    static AggregationEmitRuntime*
    beginEmit(const AggregationBuildPhysicalOperator* self, PipelineStateBuilder* state, PipelineExecutionContext* context);
    static void finishEmit(PipelineStateBuilder* state, AggregationEmitRuntime* runtime);
    static AggregationAbsorbRuntime*
    beginAbsorb(const AggregationBuildPhysicalOperator* self, PipelineStateReader* state, PipelineExecutionContext* context);

    mutable std::optional<PipelineFunction<void(TupleBuffer*, const AggregationEmitSession*, uint64_t*)>> compiledEmit;
    mutable std::optional<PipelineFunction<void(const TupleBuffer*, AggregationAbsorbSession*)>> compiledAbsorb;
    /// The aggregation function is a shared_ptr, because it is used in the aggregation build and in the getSliceCleanupFunction()
    std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationPhysicalFunctions;
    ChainedHashMapConfig hashMapConfig;
    /// Extracts the key fields out of an incoming record. Operator logic, not hash map metadata, hence not in the config.
    std::vector<PhysicalFunction> keyFunctions;
    std::optional<DonorLayout> donorLayout;
};

}
