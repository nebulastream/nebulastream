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

#include <algorithm>
#include <memory>
#include <string>
#include <Configurations/Descriptor.hpp>
#include <LogicalOperators/Operator.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <SerializableOperator.pb.h>

namespace NES::Logical
{

OperatorConcept::OperatorConcept() : id(getNextLogicalOperatorId())
{
}
OperatorConcept::OperatorConcept(OperatorId existingId) : id(existingId)
{
}

std::string NullOperator::explain(ExplainVerbosity) const
{
    PRECONDITION(false, "Calls in NullOperator are undefined");
}

std::vector<Operator> NullOperator::getChildren() const
{
    PRECONDITION(false, "Calls in NullOperator are undefined");
}

Operator NullOperator::withChildren(std::vector<Operator>) const
{
    PRECONDITION(false, "Calls in NullOperator are undefined");
}

bool NullOperator::operator==(const OperatorConcept&) const
{
    PRECONDITION(false, "Calls in NullOperator are undefined");
}

std::string_view NullOperator::getName() const noexcept
{
    PRECONDITION(false, "Calls in NullOperator are undefined");
}
NES::SerializableOperator NullOperator::serialize() const
{
    PRECONDITION(false, "Calls in NullOperator are undefined");
}
Optimizer::TraitSet NullOperator::getTraitSet() const
{
    PRECONDITION(false, "Calls in NullOperator are undefined");
}
std::vector<Schema> NullOperator::getInputSchemas() const
{
    PRECONDITION(false, "Calls in NullOperator are undefined");
}
Schema NullOperator::getOutputSchema() const
{
    PRECONDITION(false, "Calls in NullOperator are undefined");
}
std::vector<std::vector<OriginId>> NullOperator::getInputOriginIds() const
{
    PRECONDITION(false, "Calls in NullOperator are undefined");
}
std::vector<OriginId> NullOperator::getOutputOriginIds() const
{
    PRECONDITION(false, "Calls in NullOperator are undefined");
}

Operator NullOperator::withInputOriginIds(std::vector<std::vector<OriginId>>) const
{
    PRECONDITION(false, "Calls in NullOperator are undefined");
}

Operator NullOperator::withOutputOriginIds(std::vector<OriginId>) const
{
    PRECONDITION(false, "Calls in NullOperator are undefined");
}

Operator NullOperator::withInferredSchema(std::vector<Schema>) const
{
    PRECONDITION(false, "Calls in NullOperator are undefined");
}

Operator::Operator() : self(std::make_unique<Model<NullOperator>>(NullOperator{}))
{
}

Operator::Operator(const Operator& other) : self(other.self->clone())
{
}

Operator::Operator(Operator&&) noexcept = default;

Operator& Operator::operator=(const Operator& other)
{
    if (this != &other)
    {
        self = other.self->clone();
    }
    return *this;
}

std::string Operator::explain(ExplainVerbosity verbosity) const
{
    return self->explain(verbosity);
}

std::vector<Operator> Operator::getChildren() const
{
    return self->getChildren();
}

Operator Operator::withChildren(std::vector<Operator> children) const
{
    return self->withChildren(children);
}

OperatorId Operator::getId() const
{
    return self->id;
}

bool Operator::operator==(const Operator& other) const
{
    return self->equals(*other.self);
}

std::string_view Operator::getName() const noexcept
{
    return self->getName();
}

SerializableOperator Operator::serialize() const
{
    return self->serialize();
}

Optimizer::TraitSet Operator::getTraitSet() const
{
    return self->getTraitSet();
}

std::vector<Schema> Operator::getInputSchemas() const
{
    return self->getInputSchemas();
}

Schema Operator::getOutputSchema() const
{
    return self->getOutputSchema();
}

std::vector<std::vector<OriginId>> Operator::getInputOriginIds() const
{
    return self->getInputOriginIds();
}

std::vector<OriginId> Operator::getOutputOriginIds() const
{
    return self->getOutputOriginIds();
}

Operator Operator::withInputOriginIds(std::vector<std::vector<OriginId>> ids) const
{
    return self->withInputOriginIds(ids);
}

Operator Operator::withOutputOriginIds(std::vector<OriginId> ids) const
{
    return self->withOutputOriginIds(ids);
}

Operator Operator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    return self->withInferredSchema(std::move(inputSchemas));
}
}
