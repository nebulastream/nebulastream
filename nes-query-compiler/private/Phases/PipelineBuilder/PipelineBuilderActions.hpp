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

#include <iostream>
#include <memory>
#include <ranges>
#include <MemoryLayout/RowLayout.hpp>
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <Phases/PipelineBuilder/AbstractStateMachine.hpp>
#include <Phases/PipelineBuilder/PipelineBuilderState.hpp>
#include <EmitPhysicalOperator.hpp>
#include <PipelinedQueryPlan.hpp>
#include <ScanPhysicalOperator.hpp>

namespace NES
{

using Action = ActionFn<State, Event, BuilderContext>;

inline void ensureCurrentPipeline(const BuilderContext& ctx);
void addDefaultScan(BuilderContext& ctx, const PhysicalOperatorWrapper& wrappedOp);
void addDefaultEmit(BuilderContext& ctx, const PhysicalOperatorWrapper& wrappedOp);
void createPipeline(BuilderContext& ctx) noexcept;
void appendSource(BuilderContext& ctx) noexcept;
void appendSink(BuilderContext& ctx) noexcept;
void appendOperator(NES::BuilderContext& ctx) noexcept;
void prependDefaultScan(NES::BuilderContext& ctx) noexcept;
void appendDefaultEmit(NES::BuilderContext& ctx) noexcept;
void appendDefaultEmitIfNeeded(NES::BuilderContext& ctx) noexcept;
void registerHandler(NES::BuilderContext& ctx) noexcept;
void addSuccessor(BuilderContext& ctx) noexcept;
void pushContext(BuilderContext& ctx) noexcept;
void popContext(BuilderContext& ctx) noexcept;
void noop(BuilderContext&) noexcept;

}
