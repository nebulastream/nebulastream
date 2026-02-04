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

#include <SourcePhysicalOperator.hpp>

#include <memory>
#include <optional>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <CompilationContext.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

SourcePhysicalOperator::SourcePhysicalOperator(SourceDescriptor descriptor, OriginId id)
    : originId(id), descriptor(std::move(descriptor)) { };

SourceDescriptor SourcePhysicalOperator::getDescriptor() const
{
    return descriptor;
};

OriginId SourcePhysicalOperator::getOriginId() const
{
    return originId;
}

bool SourcePhysicalOperator::operator==(const SourcePhysicalOperator& other) const
{
    return descriptor == other.descriptor;
}

std::optional<PhysicalOperator> SourcePhysicalOperator::getChild() const
{
    return child;
}

SourcePhysicalOperator SourcePhysicalOperator::withChild(PhysicalOperator newChild) const
{
    auto copy = *this;
    copy.child = std::move(newChild);
    return copy;
}

void SourcePhysicalOperator::setup(ExecutionContext&, CompilationContext&) const
{
    INVARIANT(false, "Lifecycle methods must not be called on SourcePhysicalOperator");
}

void SourcePhysicalOperator::open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const
{
    openChild(ctx, recordBuffer);
}

void SourcePhysicalOperator::close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const
{
    closeChild(ctx, recordBuffer);
}

void SourcePhysicalOperator::terminate(ExecutionContext&) const
{
    INVARIANT(false, "Lifecycle methods must not be called on SourcePhysicalOperator");
}

void SourcePhysicalOperator::execute(ExecutionContext&, Record&) const
{
    INVARIANT(false, "Lifecycle methods must not be called on SourcePhysicalOperator");
}

void SourcePhysicalOperator::openChild(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    INVARIANT(child.has_value(), "Child operator is not set");
    child.value().open(executionCtx, recordBuffer);
}

void SourcePhysicalOperator::closeChild(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    INVARIANT(child.has_value(), "Child operator is not set");
    child.value().close(executionCtx, recordBuffer);
}

}
