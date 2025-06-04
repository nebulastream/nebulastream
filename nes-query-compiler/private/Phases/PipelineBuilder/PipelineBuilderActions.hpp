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
#include <MemoryLayout/RowLayout.hpp>
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <Phases/PipelineBuilder/PipelineBuilderState.hpp>
#include <EmitPhysicalOperator.hpp>
#include <PipelinedQueryPlan.hpp>
#include <ScanPhysicalOperator.hpp>

namespace NES
{

using Action = ActionFn<State, Event, BuilderContext>;

inline void ensureCurrentPipeline(const BuilderContext& ctx)
{
    INVARIANT(ctx.currentPipeline, "Action requires an active pipeline but none is set.");
}

void addDefaultScan(BuilderContext& ctx, const PhysicalOperatorWrapper& wrappedOp)
{
    ensureCurrentPipeline(ctx);
    PRECONDITION(ctx.currentPipeline->isOperatorPipeline(), "Default scan inserted only into operator pipelines.");

    auto schema = wrappedOp.getInputSchema();
    INVARIANT(schema.has_value(), "Wrapped operator has no input schema");

    auto layout = std::make_shared<Memory::MemoryLayouts::RowLayout>(ctx.bufferSize, schema.value());
    auto memProv = std::make_shared<Interface::MemoryProvider::RowTupleBufferMemoryProvider>(layout);

    ctx.currentPipeline->prependOperator(ScanPhysicalOperator(memProv, schema->getFieldNames()));
}

void addDefaultEmit(BuilderContext& ctx, const PhysicalOperatorWrapper& wrappedOp)
{
    ensureCurrentPipeline(ctx);
    PRECONDITION(ctx.currentPipeline->isOperatorPipeline(), "Default emit inserted only into operator pipelines.");

    auto schema = wrappedOp.getOutputSchema();
    INVARIANT(schema.has_value(), "Wrapped operator has no output schema");

    auto layout = std::make_shared<Memory::MemoryLayouts::RowLayout>(ctx.bufferSize, schema.value());
    auto memProv = std::make_shared<Interface::MemoryProvider::RowTupleBufferMemoryProvider>(layout);

    const OperatorHandlerId handlerId = getNextOperatorHandlerId();
    ctx.currentPipeline->getOperatorHandlers().emplace(handlerId, std::make_shared<EmitOperatorHandler>());
    ctx.currentPipeline->appendOperator(EmitPhysicalOperator(handlerId, memProv));
}

void createPipeline(BuilderContext& ctx) noexcept
{
    auto newPipe = std::make_shared<Pipeline>(ctx.currentOp->getPhysicalOperator());

    // Copy handler, if any, from the operator wrapper
    if (ctx.currentOp->getHandler() and ctx.currentOp->getHandlerId())
    {
        newPipe->getOperatorHandlers().emplace(ctx.currentOp->getHandlerId().value(), ctx.currentOp->getHandler().value());
    }

    // Register the pipeline under the operator id
    ctx.op2Pipeline.emplace(ctx.currentOp->getPhysicalOperator().getId(), newPipe);

    // If we were already in a pipeline, link it as successor,
    // otherwise this is a root pipeline and must be added to the plan.
    if (ctx.currentPipeline)
    {
        ctx.currentPipeline->addSuccessor(newPipe, ctx.currentPipeline);
    }
    else
    {
        ctx.outPlan->addPipeline(newPipe); // root
    }

    ctx.currentPipeline = std::move(newPipe);
}

void appendSource(BuilderContext& ctx) noexcept
{
    const auto source = ctx.currentOp->getPhysicalOperator();
    PRECONDITION(source.tryGet<SourcePhysicalOperator>().has_value(), "expects a source");
    auto newPipe = std::make_shared<Pipeline>(source.get<SourcePhysicalOperator>());

    if (ctx.currentPipeline)
    {
        ctx.currentPipeline->addSuccessor(newPipe, ctx.currentPipeline);
    }
    else
    {
        ctx.outPlan->addPipeline(newPipe);
    }

    ctx.currentPipeline = std::move(newPipe);
}

void appendSink(BuilderContext& ctx) noexcept
{
    const auto sink = ctx.currentOp->getPhysicalOperator();
    PRECONDITION(sink.tryGet<SinkPhysicalOperator>().has_value(), "expects a sink");
    auto newPipe = std::make_shared<Pipeline>(sink.get<SinkPhysicalOperator>());

    INVARIANT(ctx.currentPipeline, "SinkPhysicalOperator cannot be a root operator");
    ctx.outPlan->addPipeline(newPipe);

    ctx.currentPipeline = std::move(newPipe);
}


void appendOperator(NES::BuilderContext& ctx) noexcept
{
    ensureCurrentPipeline(ctx);

    ctx.currentPipeline->appendOperator(ctx.currentOp->getPhysicalOperator());

    if (ctx.currentOp->getHandler() && ctx.currentOp->getHandlerId())
    {
        ctx.currentPipeline->getOperatorHandlers().emplace(ctx.currentOp->getHandlerId().value(), ctx.currentOp->getHandler().value());
    }
}

void prependDefaultScan(NES::BuilderContext& ctx) noexcept
{
    addDefaultScan(ctx, *ctx.currentOp);
}

void appendDefaultEmit(NES::BuilderContext& ctx) noexcept
{
    addDefaultEmit(ctx, ctx.prevOp ? *ctx.prevOp : *ctx.currentOp);
}

void registerHandler(NES::BuilderContext& ctx) noexcept
{
    if (!ctx.currentPipeline)
    {
        return;
    }

    if (ctx.currentOp->getHandler() && ctx.currentOp->getHandlerId())
    {
        ctx.currentPipeline->getOperatorHandlers().emplace(ctx.currentOp->getHandlerId().value(), ctx.currentOp->getHandler().value());
    }
}

void addSuccessor(BuilderContext& ctx) noexcept
{
    ensureCurrentPipeline(ctx);

    const auto opId = ctx.currentOp->getPhysicalOperator().getId();
    auto it = ctx.op2Pipeline.find(opId);
    INVARIANT(it != ctx.op2Pipeline.end(), "Successor pipeline not found in op2Pipeline map.");

    ctx.currentPipeline->addSuccessor(it->second, ctx.currentPipeline);
}

void pushContext(BuilderContext& ctx) noexcept
{
    ctx.contextStack.push_back(Frame{ctx.currentOp, ctx.prevOp, ctx.currentPipeline});
}

void popContext(BuilderContext& ctx) noexcept
{
    INVARIANT(!ctx.contextStack.empty(), "Context stack underflow in pop.");

    const Frame top = ctx.contextStack.back();
    ctx.contextStack.pop_back();

    ctx.currentOp = top.op;
    ctx.prevOp = top.prev;
    ctx.currentPipeline = top.pipeline;
}

void noop(BuilderContext&) noexcept
{ /* noop */
}

}
