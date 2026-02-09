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

#include <SinkPhysicalOperator.hpp>

#include <memory>
#include <optional>
#include <utility>
#include <Sinks/SinkDescriptor.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

SinkPhysicalOperator::SinkPhysicalOperator(const SinkDescriptor& descriptor) : descriptor(descriptor) { };

SinkDescriptor SinkPhysicalOperator::getDescriptor() const
{
    return descriptor;
};

bool SinkPhysicalOperator::operator==(const SinkPhysicalOperator& other) const
{
    return descriptor == other.descriptor;
}

std::optional<PhysicalOperator> SinkPhysicalOperator::getChild() const
{
    return std::nullopt;
}

SinkPhysicalOperator SinkPhysicalOperator::withChild(PhysicalOperator) const
{
    PRECONDITION(false, "Sink has no child.");
}

void SinkPhysicalOperator::setup(ExecutionContext& ctx, CompilationContext& compCtx) const
{
    setupChild(ctx, compCtx);
}

void SinkPhysicalOperator::open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const
{
    openChild(ctx, recordBuffer);
}

void SinkPhysicalOperator::close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const
{
    closeChild(ctx, recordBuffer);
}

void SinkPhysicalOperator::terminate(ExecutionContext& ctx) const
{
    terminateChild(ctx);
}

void SinkPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    executeChild(ctx, record);
}

OperatorId SinkPhysicalOperator::getId() const
{
    return id;
}

void SinkPhysicalOperator::setupChild(ExecutionContext&, CompilationContext&) const
{
    INVARIANT(false, "Child operator is not set");
}

void SinkPhysicalOperator::openChild(ExecutionContext&, RecordBuffer&) const
{
    INVARIANT(false, "Child operator is not set");
}

void SinkPhysicalOperator::closeChild(ExecutionContext&, RecordBuffer&) const
{
    INVARIANT(false, "Child operator is not set");
}

void SinkPhysicalOperator::executeChild(ExecutionContext&, Record&) const
{
    INVARIANT(false, "Child operator is not set");;
}

void SinkPhysicalOperator::terminateChild(ExecutionContext&) const
{
    INVARIANT(false, "Child operator is not set");
}

}
