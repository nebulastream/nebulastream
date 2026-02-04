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
#include <SelectionPhysicalOperator.hpp>

#include <optional>
#include <utility>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <CompilationContext.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

std::optional<PhysicalOperator> SelectionPhysicalOperator::getChild() const
{
    return child;
}

SelectionPhysicalOperator SelectionPhysicalOperator::withChild(PhysicalOperator newChild) const
{
    auto copy = *this;
    copy.child = std::move(newChild);
    return copy;
}

void SelectionPhysicalOperator::setup(ExecutionContext& ctx, CompilationContext& compCtx) const
{
    setupChild(ctx, compCtx);
}

void SelectionPhysicalOperator::open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const
{
    openChild(ctx, recordBuffer);
}

void SelectionPhysicalOperator::close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const
{
    closeChild(ctx, recordBuffer);
}

void SelectionPhysicalOperator::terminate(ExecutionContext& ctx) const
{
    terminateChild(ctx);
}

void SelectionPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    /// evaluate function and call child operator if function is valid
    if (function.execute(record, ctx.pipelineMemoryProvider.arena))
    {
        executeChild(ctx, record);
    }
}

void SelectionPhysicalOperator::setupChild(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    INVARIANT(child.has_value(), "Child operator is not set");
    child.value().setup(executionCtx, compilationContext);
}

void SelectionPhysicalOperator::openChild(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    INVARIANT(child.has_value(), "Child operator is not set");
    child.value().open(executionCtx, recordBuffer);
}

void SelectionPhysicalOperator::closeChild(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    INVARIANT(child.has_value(), "Child operator is not set");
    child.value().close(executionCtx, recordBuffer);
}

void SelectionPhysicalOperator::executeChild(ExecutionContext& executionCtx, Record& record) const
{
    INVARIANT(child.has_value(), "Child operator is not set");
    child.value().execute(executionCtx, record);
}

void SelectionPhysicalOperator::terminateChild(ExecutionContext& executionCtx) const
{
    INVARIANT(child.has_value(), "Child operator is not set");
    child.value().terminate(executionCtx);
}

}
