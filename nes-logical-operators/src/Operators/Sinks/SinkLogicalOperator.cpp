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

#include <Operators/Sinks/SinkLogicalOperator.hpp>

#include <algorithm>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{

SinkLogicalOperator::SinkLogicalOperator(std::string sinkName) : sinkName(std::move(sinkName)) { };

SinkLogicalOperator::SinkLogicalOperator(Sinks::SinkDescriptor sinkDescriptor)
    : sinkName(sinkDescriptor.getSinkName()), sinkDescriptor(std::move(sinkDescriptor))
{
}

bool SinkLogicalOperator::operator==(const LogicalOperatorConcept& rhs) const
{
    if (const auto* rhsOperator = dynamic_cast<const SinkLogicalOperator*>(&rhs))
    {
        const bool descriptorsEqual = (not sinkDescriptor.has_value() && not rhsOperator->sinkDescriptor.has_value())
            || (sinkDescriptor.has_value() && rhsOperator->sinkDescriptor.has_value() && *sinkDescriptor == *rhsOperator->sinkDescriptor);

        return sinkName == rhsOperator->sinkName && descriptorsEqual && getOutputSchema() == rhsOperator->getOutputSchema()
            && getInputSchemas() == rhsOperator->getInputSchemas() && getInputOriginIds() == rhsOperator->getInputOriginIds()
            && getOutputOriginIds() == rhsOperator->getOutputOriginIds();
    }
    return false;
}


std::string SinkLogicalOperator::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "SINK(opId: {}, sinkName: {}, sinkDescriptor: {})",
            id,
            sinkName,
            (sinkDescriptor) ? fmt::format("{}", *sinkDescriptor) : "(null)");
    }
    return fmt::format("SINK({})", sinkName);
}

std::string_view SinkLogicalOperator::getName() const noexcept
{
    return NAME;
}

LogicalOperator SinkLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    auto copy = *this;
    INVARIANT(!inputSchemas.empty(), "Sink should have at least one input");

    const auto& firstSchema = inputSchemas[0];
    for (const auto& schema : inputSchemas)
    {
        if (schema != firstSchema)
        {
            throw CannotInferSchema("All input schemas must be equal for Sink operator");
        }
    }

    if (copy.sinkDescriptor.has_value() && *copy.sinkDescriptor->getSchema() != firstSchema)
    {
        std::vector expectedFields(copy.sinkDescriptor.value().getSchema()->begin(), copy.sinkDescriptor.value().getSchema()->end());
        std::vector actualFields(firstSchema.begin(), firstSchema.end());

        std::stringstream expectedFieldsString;
        std::stringstream actualFieldsString;

        for (const auto& field : firstSchema)
        {
            if (std::ranges::find(expectedFields, field) == expectedFields.end())
            {
                expectedFieldsString << field << ", ";
            }
        }
        for (const auto& field : *copy.sinkDescriptor.value().getSchema())
        {
            if (std::ranges::find(actualFields, field) == actualFields.end())
            {
                actualFieldsString << field << ", ";
            }
        }

        throw CannotInferSchema(
            "The schema of the sink must be equal to the schema of the input. Expected fields {}where not found, and found unexpected "
            "fields {}",
            expectedFieldsString.str(),
            actualFieldsString.str().substr(0, actualFieldsString.str().size() - 2));
    }
    return copy;
}

TraitSet SinkLogicalOperator::getTraitSet() const
{
    return {};
}

LogicalOperator SinkLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> SinkLogicalOperator::getInputSchemas() const
{
    return children | std::ranges::views::transform([](const LogicalOperator& child) { return child.getOutputSchema(); })
        | std::ranges::to<std::vector>();
};

Schema SinkLogicalOperator::getOutputSchema() const
{
    INVARIANT(!children.empty(), "Sink should have one child");
    return children.at(0).getOutputSchema();
}

std::vector<std::vector<OriginId>> SinkLogicalOperator::getInputOriginIds() const
{
    return {inputOriginIds};
}

std::vector<OriginId> SinkLogicalOperator::getOutputOriginIds() const
{
    return outputOriginIds;
}

LogicalOperator SinkLogicalOperator::withInputOriginIds(std::vector<std::vector<OriginId>> ids) const
{
    auto copy = *this;
    copy.inputOriginIds = ids[0];
    return copy;
}

LogicalOperator SinkLogicalOperator::withOutputOriginIds(std::vector<OriginId> ids) const
{
    auto copy = *this;
    copy.outputOriginIds = ids;
    return copy;
}

std::vector<LogicalOperator> SinkLogicalOperator::getChildren() const
{
    return children;
}

SerializableOperator SinkLogicalOperator::serialize() const
{
    SerializableSinkLogicalOperator proto;
    if (sinkDescriptor)
    {
        proto.mutable_sinkdescriptor()->CopyFrom(sinkDescriptor->serialize());
    }

    SerializableOperator serializableOperator;
    const Configurations::DescriptorConfig::ConfigType timeVariant = sinkName;
    (*serializableOperator.mutable_config())[ConfigParameters::SINK_NAME] = descriptorConfigTypeToProto(timeVariant);

    serializableOperator.set_operator_id(id.getRawValue());
    for (auto& child : getChildren())
    {
        serializableOperator.add_children_ids(child.getId().getRawValue());
    }

    serializableOperator.mutable_sink()->CopyFrom(proto);
    return serializableOperator;
}
}
