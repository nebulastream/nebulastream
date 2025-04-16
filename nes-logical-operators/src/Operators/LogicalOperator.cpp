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
#include <Functions/FieldAssignmentLogicalFunction.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/MapLogicalOperator.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableSchema.pb.h>

namespace NES
{

LogicalOperatorConcept::LogicalOperatorConcept() : id(getNextOperatorId())
{
}
LogicalOperatorConcept::LogicalOperatorConcept(OperatorId existingId) : id(existingId)
{
}

std::string NullLogicalOperator::toString() const
{
    PRECONDITION(false, "Calls in NullLogicalOperator are undefined");
}

std::vector<NES::LogicalOperator> NullLogicalOperator::getChildren() const
{
    PRECONDITION(false, "Calls in NullLogicalOperator are undefined");
}

LogicalOperator NullLogicalOperator::withChildren(std::vector<NES::LogicalOperator>) const
{
    PRECONDITION(false, "Calls in NullLogicalOperator are undefined");
}

bool NullLogicalOperator::operator==(const NES::LogicalOperatorConcept&) const
{
    return false;
}

std::string_view NullLogicalOperator::getName() const noexcept
{
    PRECONDITION(false, "Calls in NullLogicalOperator are undefined");
}
NES::SerializableOperator NullLogicalOperator::serialize() const
{
    PRECONDITION(false, "Calls in NullLogicalOperator are undefined");
}
Optimizer::TraitSet NullLogicalOperator::getTraitSet() const
{
    PRECONDITION(false, "Calls in NullLogicalOperator are undefined");
}
std::vector<Schema> NullLogicalOperator::getInputSchemas() const
{
    PRECONDITION(false, "Calls in NullLogicalOperator are undefined");
}
Schema NullLogicalOperator::getOutputSchema() const
{
    PRECONDITION(false, "Calls in NullLogicalOperator are undefined");
}
std::vector<std::vector<OriginId>> NullLogicalOperator::getInputOriginIds() const
{
    PRECONDITION(false, "Calls in NullLogicalOperator are undefined");
}
std::vector<OriginId> NullLogicalOperator::getOutputOriginIds() const
{
    PRECONDITION(false, "Calls in NullLogicalOperator are undefined");
}

LogicalOperator NullLogicalOperator::withInputOriginIds(std::vector<std::vector<OriginId>>) const
{
    PRECONDITION(false, "Calls in NullLogicalOperator are undefined");
}

LogicalOperator NullLogicalOperator::withOutputOriginIds(std::vector<OriginId>) const
{
    PRECONDITION(false, "Calls in NullLogicalOperator are undefined");
}

LogicalOperator NullLogicalOperator::withInferredSchema(std::vector<Schema>) const
{
    PRECONDITION(false, "Calls in NullLogicalOperator are undefined");
}

LogicalOperator::LogicalOperator() : self(std::make_unique<Model<NullLogicalOperator>>(NullLogicalOperator{}))
{
}

LogicalOperator::LogicalOperator(const LogicalOperator& other) : self(other.self->clone())
{
}

LogicalOperator::LogicalOperator(LogicalOperator&&) noexcept = default;

LogicalOperator& LogicalOperator::operator=(const LogicalOperator& other)
{
    if (this != &other)
    {
        self = other.self->clone();
    }
    return *this;
}

std::string LogicalOperator::toString() const
{
    return self->toString();
}

std::vector<LogicalOperator> LogicalOperator::getChildren() const
{
    return self->getChildren();
}

LogicalOperator LogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    return self->withChildren(children);
}

OperatorId LogicalOperator::getId() const
{
    return self->id;
}

bool LogicalOperator::operator==(const LogicalOperator& other) const
{
    return self->equals(*other.self);
}

std::string_view LogicalOperator::getName() const noexcept
{
    return self->getName();
}

SerializableOperator LogicalOperator::serialize() const
{
    return self->serialize();
}

Optimizer::TraitSet LogicalOperator::getTraitSet() const
{
    return self->getTraitSet();
}

std::vector<Schema> LogicalOperator::getInputSchemas() const
{
    return self->getInputSchemas();
}

Schema LogicalOperator::getOutputSchema() const
{
    return self->getOutputSchema();
}

std::vector<std::vector<OriginId>> LogicalOperator::getInputOriginIds() const
{
    return self->getInputOriginIds();
}

std::vector<OriginId> LogicalOperator::getOutputOriginIds() const
{
    return self->getOutputOriginIds();
}

LogicalOperator LogicalOperator::withInputOriginIds(std::vector<std::vector<OriginId>> ids) const
{
    return self->withInputOriginIds(ids);
}

LogicalOperator LogicalOperator::withOutputOriginIds(std::vector<OriginId> ids) const
{
    return self->withOutputOriginIds(ids);
}

LogicalOperator LogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    return self->withInferredSchema(std::move(inputSchemas));
}
}
