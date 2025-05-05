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

#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{


PhysicalOperatorConcept::PhysicalOperatorConcept() : id(getNextPhysicalOperatorId())
{
}
PhysicalOperatorConcept::PhysicalOperatorConcept(OperatorId existingId) : id(existingId)
{
}


void PhysicalOperatorConcept::setup(ExecutionContext& executionCtx) const
{
    if (getChild())
    {
        getChild().value().setup(executionCtx);
    }
}

void PhysicalOperatorConcept::open(ExecutionContext& executionCtx, RecordBuffer& rb) const
{
    if (getChild())
    {
        getChild().value().open(executionCtx, rb);
    }
}

void PhysicalOperatorConcept::close(ExecutionContext& executionCtx, RecordBuffer& rb) const
{
    if (getChild())
    {
        getChild().value().close(executionCtx, rb);
    }
}

void PhysicalOperatorConcept::terminate(ExecutionContext& executionCtx) const
{
    if (getChild())
    {
        getChild().value().terminate(executionCtx);
    }
}

void PhysicalOperatorConcept::execute(NES::ExecutionContext& executionCtx, NES::Nautilus::Record& record) const
{
    if (getChild())
    {
        getChild().value().execute(executionCtx, record);
    }
}

std::string PhysicalOperatorConcept::toString() const
{
    return "PhysicalOperatorConcept";
}

PhysicalOperator::PhysicalOperator(const PhysicalOperator& other) : self(other.self->clone())
{
}

PhysicalOperator::PhysicalOperator(PhysicalOperator&&) noexcept = default;

PhysicalOperator& PhysicalOperator::operator=(const PhysicalOperator& other)
{
    if (this != &other)
    {
        self = other.self->clone();
    }
    return *this;
}

std::optional<PhysicalOperator> PhysicalOperator::getChild() const
{
    return self->getChild();
}

void PhysicalOperator::setChild(PhysicalOperator child)
{
    self->setChild(child);
}

void PhysicalOperator::setup(ExecutionContext& executionCtx) const
{
    self->setup(executionCtx);
}

void PhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    self->open(executionCtx, recordBuffer);
}

void PhysicalOperator::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    self->close(executionCtx, recordBuffer);
}

void PhysicalOperator::terminate(ExecutionContext& executionCtx) const
{
    self->terminate(executionCtx);
}

void PhysicalOperator::execute(ExecutionContext& executionCtx, Record& record) const
{
    self->execute(executionCtx, record);
}

std::string PhysicalOperator::toString() const
{
    return self->toString();
}

[[nodiscard]] OperatorId PhysicalOperator::getId() const
{
    return self->id;
}

PhysicalOperatorWrapper::PhysicalOperatorWrapper(PhysicalOperator physicalOperator, Schema inputSchema, Schema outputSchema)
    : physicalOperator(physicalOperator), inputSchema(inputSchema), outputSchema(outputSchema) {};

std::string PhysicalOperatorWrapper::explain(ExplainVerbosity) const
{
    return fmt::format(
        "PhysicalOperatorWrapper("
        "Operator: {}, id: {}, "
        "InputSchema: {}, OutputSchema: {}, "
        "isScan: {}, isEmit: {})",
        physicalOperator.toString(),
        physicalOperator.getId(),
        inputSchema ? inputSchema->toString() : "none",
        outputSchema ? outputSchema->toString() : "none",
        isScan,
        isEmit
    );
}


}
