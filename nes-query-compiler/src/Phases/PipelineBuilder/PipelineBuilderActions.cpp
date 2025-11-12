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

#include <Phases/PipelineBuilder/PipelineBuilderActions.hpp>

namespace NES
{

void addDefaultScan(BuilderContext& ctx, const PhysicalOperatorWrapper& wrappedOp)
{
    INVARIANT(ctx.currentPipeline, "Action requires an active pipeline but none is set.");
    PRECONDITION(ctx.currentPipeline->isOperatorPipeline(), "Default scan inserted only into operator pipelines.");

    auto schema = wrappedOp.getInputSchema();
    INVARIANT(schema.has_value(), "Wrapped operator has no input schema");

    auto layout = std::make_shared<RowLayout>(ctx.bufferSize, schema.value());
    auto memProv = std::make_shared<Interface::BufferRef::RowTupleBufferRef>(layout);

    ctx.currentPipeline->prependOperator(ScanPhysicalOperator(memProv, schema->getFieldNames()));
}

void addDefaultEmit(BuilderContext& ctx, const PhysicalOperatorWrapper& wrappedOp)
{
    INVARIANT(ctx.currentPipeline, "Action requires an active pipeline but none is set.");
    PRECONDITION(ctx.currentPipeline->isOperatorPipeline(), "Default emit inserted only into operator pipelines.");

    auto schema = wrappedOp.getOutputSchema();
    INVARIANT(schema.has_value(), "Wrapped operator has no output schema");

    auto layout = std::make_shared<RowLayout>(ctx.bufferSize, schema.value());
    auto memProv = std::make_shared<Interface::BufferRef::RowTupleBufferRef>(layout);

    const OperatorHandlerId handlerId = getNextOperatorHandlerId();
    ctx.currentPipeline->getOperatorHandlers().emplace(handlerId, std::make_shared<EmitOperatorHandler>());
    ctx.currentPipeline->appendOperator(EmitPhysicalOperator(handlerId, memProv));
}

void createPipeline(BuilderContext& ctx) noexcept
{
    const auto opId = ctx.currentOp->getPhysicalOperator().getId();

    /// If a pipeline for this operator id already exists
    std::shared_ptr<Pipeline> pipeline;
    if (auto it = ctx.op2Pipeline.find(opId); it != ctx.op2Pipeline.end())
    {
        pipeline = it->second;
    }
    else
    {
        pipeline = std::make_shared<Pipeline>(ctx.currentOp->getPhysicalOperator());

        /// Copy handler, if any, from the operator wrapper
        if (ctx.currentOp->getHandler() && ctx.currentOp->getHandlerId())
        {
            pipeline->getOperatorHandlers().emplace(ctx.currentOp->getHandlerId().value(), ctx.currentOp->getHandler().value());
        }

        /// Register the pipeline under the operator id
        ctx.op2Pipeline.emplace(opId, pipeline);
    }

    /// If we were already in a pipeline, link it as successor,
    /// otherwise this is a root pipeline and must be added to the plan.
    if (ctx.currentPipeline)
    {
        const bool alreadyLinked = std::ranges::any_of(ctx.currentPipeline->getSuccessors(), [&](const auto& s) { return s == pipeline; });
        if (!alreadyLinked)
            ctx.currentPipeline->addSuccessor(pipeline, ctx.currentPipeline);
    }
    else
    {
        const bool alreadyRoot = std::ranges::any_of(ctx.outPlan->getPipelines(), [&](const auto& p) { return p == pipeline; });
        if (!alreadyRoot)
            ctx.outPlan->addPipeline(pipeline);
    }

    ctx.currentPipeline = pipeline;
}

void appendSource(BuilderContext& ctx) noexcept
{
    const auto source = ctx.currentOp->getPhysicalOperator();
    PRECONDITION(source.tryGet<SourcePhysicalOperator>().has_value(), "expects a source");
    auto newPipe = std::make_shared<Pipeline>(source.get<SourcePhysicalOperator>());
    ctx.op2Pipeline.emplace(source.getId(), newPipe);

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
    const auto sinkOp = ctx.currentOp->getPhysicalOperator();
    const auto sinkId = sinkOp.getId();
    PRECONDITION(sinkOp.tryGet<SinkPhysicalOperator>().has_value(), "expects a sink");
    INVARIANT(ctx.currentPipeline, "SinkPhysicalOperator cannot be a root operator");

    std::shared_ptr<Pipeline> sinkPipeline;

    if (auto it = ctx.op2Pipeline.find(sinkId); it != ctx.op2Pipeline.end())
    {
        sinkPipeline = it->second;
    }
    else
    {
        sinkPipeline = std::make_shared<Pipeline>(sinkOp.get<SinkPhysicalOperator>());
        ctx.op2Pipeline.emplace(sinkId, sinkPipeline);

        if (ctx.currentOp->getHandler() && ctx.currentOp->getHandlerId())
        {
            sinkPipeline->getOperatorHandlers().emplace(ctx.currentOp->getHandlerId().value(), ctx.currentOp->getHandler().value());
        }
    }

    const bool alreadyLinked
        = std::ranges::any_of(ctx.currentPipeline->getSuccessors(), [&](const auto& s) { return s.get() == sinkPipeline.get(); });
    if (!alreadyLinked)
    {
        ctx.currentPipeline->addSuccessor(sinkPipeline, ctx.currentPipeline);
    }

    ctx.currentPipeline = sinkPipeline;
}

void appendOperator(NES::BuilderContext& ctx) noexcept
{
    INVARIANT(ctx.currentPipeline, "Action requires an active pipeline but none is set.");

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

void appendDefaultEmitIfNeeded(NES::BuilderContext& ctx) noexcept
{
    if (ctx.prevOp && ctx.prevOp->getPipelineLocation() != PhysicalOperatorWrapper::PipelineLocation::EMIT)
    {
        addDefaultEmit(ctx, *ctx.prevOp);
    }
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
    INVARIANT(ctx.currentPipeline, "Action requires an active pipeline but none is set.");

    const auto opId = ctx.currentOp->getPhysicalOperator().getId();
    auto it = ctx.op2Pipeline.find(opId);
    INVARIANT(it != ctx.op2Pipeline.end(), "Successor pipeline not found in op2Pipeline map.");

    const auto& successor = it->second;

    /// prevent self-loop
    if (successor.get() == ctx.currentPipeline.get())
    {
        return;
    }

    /// avoid duplicate edges
    const bool exists
        = std::ranges::any_of(ctx.currentPipeline->getSuccessors(), [&](const auto& s) { return s.get() == successor.get(); });
    if (exists)
    {
        return;
    }

    ctx.currentPipeline->addSuccessor(successor, ctx.currentPipeline);
}

void pushContext(BuilderContext& ctx) noexcept
{
    auto& parentFrame = ctx.contextStack.back();
    auto nextIdx = parentFrame.nextChildIdx++;

    PRECONDITION(nextIdx < parentFrame.op->getChildren().size(), "pushContext called but no remaining child");

    auto child = parentFrame.op->getChildren()[nextIdx];
    ctx.contextStack.push_back(Frame{child, parentFrame.op, ctx.currentPipeline, 0});
}

void popContext(BuilderContext& ctx) noexcept
{
    INVARIANT(!ctx.contextStack.empty(), "Context stack underflow in pop.");

    auto childPipeline = ctx.currentPipeline;

    ctx.contextStack.pop_back();

    if (!ctx.contextStack.empty())
    {
        const Frame& parent = ctx.contextStack.back();
        ctx.currentOp = parent.op;
        ctx.prevOp = parent.prev;
        ctx.currentPipeline = childPipeline;
    }
    else
    {
        ctx.currentOp.reset();
        ctx.prevOp.reset();
        ctx.currentPipeline.reset();
    }
}

void noop(BuilderContext&) noexcept
{ /* noop */
}
}
