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

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{

LogicalOperatorConcept::LogicalOperatorConcept() : id(getNextLogicalOperatorId())
{
}
LogicalOperatorConcept::LogicalOperatorConcept(OperatorId existingId) : id(existingId)
{
}

LogicalOperator::LogicalOperator() : self(nullptr)
{
}

std::string LogicalOperator::explain(ExplainVerbosity verbosity) const
{
    return self->explain(verbosity);
}

std::vector<LogicalOperator> LogicalOperator::getChildren() const
{
    return self->getChildren();
}

LogicalOperator LogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    return self->withChildren(std::move(children));
}

LogicalOperator LogicalOperator::withTraitSet(TraitSet traitSet) const
{
    return self->withTraitSet(std::move(traitSet));
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

TraitSet LogicalOperator::getTraitSet() const
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
    return self->withInputOriginIds(std::move(ids));
}

LogicalOperator LogicalOperator::withOutputOriginIds(std::vector<OriginId> ids) const
{
    return self->withOutputOriginIds(std::move(ids));
}

LogicalOperator LogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    return self->withInferredSchema(std::move(inputSchemas));
}
}
