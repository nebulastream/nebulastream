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

#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <ErrorHandling.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{
SourceDescriptorLogicalOperator::SourceDescriptorLogicalOperator(SourceDescriptor sourceDescriptor)
    : sourceDescriptor(std::move(sourceDescriptor))
{
}

std::string_view SourceDescriptorLogicalOperator::getName() const noexcept
{
    return NAME;
}

LogicalOperator SourceDescriptorLogicalOperator::withInferredSchema(std::vector<Schema>) const
{
    PRECONDITION(false, "Schema is already given by SourceDescriptor. No call ot InferSchema needed");
    return *this;
}

bool SourceDescriptorLogicalOperator::operator==(const LogicalOperatorConcept& rhs) const
{
    if (const auto* const rhsOperator = dynamic_cast<const SourceDescriptorLogicalOperator*>(&rhs))
    {
        const bool descriptorsEqual = sourceDescriptor == rhsOperator->sourceDescriptor;
        return descriptorsEqual && getOutputSchema() == rhsOperator->getOutputSchema()
            && getInputSchemas() == rhsOperator->getInputSchemas() && getInputOriginIds() == rhsOperator->getInputOriginIds()
            && getOutputOriginIds() == rhsOperator->getOutputOriginIds() && getTraitSet() == rhsOperator->getTraitSet();
    }
    return false;
}

std::string SourceDescriptorLogicalOperator::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "SOURCE(opId: {}, originids: {}, {})", id, fmt::join(sourceOriginIds, ", "), sourceDescriptor.explain(verbosity));
    }
    return fmt::format("SOURCE({})", sourceDescriptor.explain(verbosity));
}

LogicalOperator SourceDescriptorLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = traitSet;
    return copy;
}

TraitSet SourceDescriptorLogicalOperator::getTraitSet() const
{
    TraitSet result = traitSet;
    result.insert(originIdTrait);
    return result;
}

LogicalOperator SourceDescriptorLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> SourceDescriptorLogicalOperator::getInputSchemas() const
{
    return {*sourceDescriptor.getLogicalSource().getSchema()};
};

Schema SourceDescriptorLogicalOperator::getOutputSchema() const
{
    return {*sourceDescriptor.getLogicalSource().getSchema()};
}

std::vector<std::vector<OriginId>> SourceDescriptorLogicalOperator::getInputOriginIds() const
{
    return {sourceOriginIds};
}

std::vector<OriginId> SourceDescriptorLogicalOperator::getOutputOriginIds() const
{
    return sourceOriginIds;
}

LogicalOperator SourceDescriptorLogicalOperator::withInputOriginIds(std::vector<std::vector<OriginId>> ids) const
{
    PRECONDITION(ids.size() == 1, "Source should have one input");
    PRECONDITION(ids[0].size() == 1, "Source should have one originId, but has {}", ids[0].size());
    auto copy = *this;
    copy.sourceOriginIds = ids.at(0);
    return copy;
}

LogicalOperator SourceDescriptorLogicalOperator::withOutputOriginIds(std::vector<OriginId> ids) const
{
    auto copy = *this;
    copy.sourceOriginIds = ids;
    return copy;
}

std::vector<LogicalOperator> SourceDescriptorLogicalOperator::getChildren() const
{
    return children;
}

SourceDescriptor SourceDescriptorLogicalOperator::getSourceDescriptor() const
{
    return sourceDescriptor;
}

void SourceDescriptorLogicalOperator::serialize(SerializableOperator& serializableOperator) const
{
    SerializableSourceDescriptorLogicalOperator proto;
    INVARIANT(sourceOriginIds.size() == 1, "Expected one originId, got '{}' instead", sourceOriginIds.size());
    proto.set_sourceoriginid(sourceOriginIds[0].getRawValue());
    proto.mutable_sourcedescriptor()->CopyFrom(sourceDescriptor.serialize());

    serializableOperator.set_operator_id(id.getRawValue());

    serializableOperator.mutable_source()->CopyFrom(proto);
}

}
