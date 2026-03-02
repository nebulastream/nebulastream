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
#include <UnionPhysicalOperator.hpp>

#include <optional>
#include <utility>
#include <Nautilus/Interface/Record.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{


std::optional<PhysicalOperator> UnionPhysicalOperator::getChild() const
{
    return child;
}

UnionPhysicalOperator UnionPhysicalOperator::withChild(PhysicalOperator newChild) const
{
    auto copy = *this;
    copy.child = std::move(newChild);
    return copy;
}

void UnionPhysicalOperator::setup(ExecutionContext& ctx, CompilationContext& compCtx) const
{
    setupChild(ctx, compCtx);
}

void UnionPhysicalOperator::open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const
{
    openChild(ctx, recordBuffer);
}

void UnionPhysicalOperator::close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const
{
    closeChild(ctx, recordBuffer);
}

void UnionPhysicalOperator::terminate(ExecutionContext& ctx) const
{
    terminateChild(ctx);
}

void UnionPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    /// Path-through, will be optimized out during query compilation
    executeChild(ctx, record);
}

OperatorId UnionPhysicalOperator::getId() const
{
    return id;
}

void UnionPhysicalOperator::setupChild(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    INVARIANT(child.has_value(), "Child operator is not set");
    child.value().setup(executionCtx, compilationContext);
}

void UnionPhysicalOperator::openChild(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    INVARIANT(child.has_value(), "Child operator is not set");
    child.value().open(executionCtx, recordBuffer);
}

void UnionPhysicalOperator::closeChild(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    INVARIANT(child.has_value(), "Child operator is not set");
    child.value().close(executionCtx, recordBuffer);
}

void UnionPhysicalOperator::executeChild(ExecutionContext& executionCtx, Record& record) const
{
    INVARIANT(child.has_value(), "Child operator is not set");
    child.value().execute(executionCtx, record);
}

void UnionPhysicalOperator::terminateChild(ExecutionContext& executionCtx) const
{
    INVARIANT(child.has_value(), "Child operator is not set");
    child.value().terminate(executionCtx);
}

}
