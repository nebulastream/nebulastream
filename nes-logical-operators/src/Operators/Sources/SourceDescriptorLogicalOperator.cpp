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

#include <sstream>
#include <utility>
#include <API/Schema.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include "Configurations/Descriptor.hpp"

namespace NES
{
SourceDescriptorLogicalOperator::SourceDescriptorLogicalOperator(std::shared_ptr<Sources::SourceDescriptor>&& sourceDescriptor)
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
}

bool SourceDescriptorLogicalOperator::operator==(const LogicalOperatorConcept& rhs) const
{
    if (const auto rhsOperator = dynamic_cast<const SourceDescriptorLogicalOperator*>(&rhs))
    {
        return *getSourceDescriptor() == *rhsOperator->getSourceDescriptor();
    }
    return false;
}

std::string SourceDescriptorLogicalOperator::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("SOURCE(opId: {}, originid: {}, {})", id, outputOriginIds, sourceDescriptor->explain(verbosity));
    }
    return fmt::format("SOURCE({})", sourceDescriptor->explain(verbosity));
}

Optimizer::TraitSet SourceDescriptorLogicalOperator::getTraitSet() const
{
    return {originIdTrait};
}

LogicalOperator SourceDescriptorLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> SourceDescriptorLogicalOperator::getInputSchemas() const
{
    return {sourceDescriptor->schema};
};

Schema SourceDescriptorLogicalOperator::getOutputSchema() const
{
    return sourceDescriptor->schema;
}

std::vector<std::vector<OriginId>> SourceDescriptorLogicalOperator::getInputOriginIds() const
{
    return {inputOriginIds};
}

std::vector<OriginId> SourceDescriptorLogicalOperator::getOutputOriginIds() const
{
    return outputOriginIds;
}

LogicalOperator SourceDescriptorLogicalOperator::withInputOriginIds(std::vector<std::vector<OriginId>> ids) const
{
    PRECONDITION(ids.size() == 1, "Source should have one input");
    PRECONDITION(ids[0].size() == 1, "Source should have one originId, but has {}", ids[0].size());
    auto copy = *this;
    copy.inputOriginIds = ids[0];
    return copy;
}

LogicalOperator SourceDescriptorLogicalOperator::withOutputOriginIds(std::vector<OriginId> ids) const
{
    auto copy = *this;
    copy.outputOriginIds = ids;
    return copy;
}

std::vector<LogicalOperator> SourceDescriptorLogicalOperator::getChildren() const
{
    return children;
}

std::shared_ptr<Sources::SourceDescriptor> SourceDescriptorLogicalOperator::getSourceDescriptor() const
{
    return sourceDescriptor;
}

Sources::SourceDescriptor& SourceDescriptorLogicalOperator::getSourceDescriptorRef() const
{
    return *sourceDescriptor;
}

[[nodiscard]] SerializableOperator SourceDescriptorLogicalOperator::serialize() const
{
    SerializableOperator_SourceDescriptorLogicalOperator proto;
    INVARIANT(outputOriginIds.size() == 1, "Expected one output originId, got '{}' instead", outputOriginIds.size());
    proto.set_sourceoriginid(outputOriginIds[0].getRawValue());
    proto.mutable_sourcedescriptor()->CopyFrom(sourceDescriptor->serialize());

    SerializableOperator serializableOperator;
    serializableOperator.set_operator_id(id.getRawValue());

    serializableOperator.mutable_source()->CopyFrom(proto);
    return serializableOperator;
}

}
