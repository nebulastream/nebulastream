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
#include <memory>
#include <optional>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{

SinkLogicalOperator::SinkLogicalOperator(std::string sinkName) : sinkName(std::move(sinkName)) { };

SinkLogicalOperator::SinkLogicalOperator(SinkDescriptor sinkDescriptor)
    : sinkName(sinkDescriptor.getSinkName()), sinkDescriptor(std::move(sinkDescriptor))
{
}

bool SinkLogicalOperator::operator==(const SinkLogicalOperator& rhs) const
{
    bool result = true;
    result &= sinkName == rhs.sinkName;
    result &= sinkDescriptor == rhs.sinkDescriptor;
    result &= getTraitSet() == rhs.getTraitSet();
    if (sinkDescriptor)
    {
        result &= getOutputSchema() == rhs.getOutputSchema();
        result &= getInputSchemas() == rhs.getInputSchemas();
    }

    return result;
}

std::string SinkLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        if (sinkDescriptor.has_value())
        {
            return fmt::format(
                "SINK(opId: {}, sinkName: {}, sinkDescriptor: {}, schema: {}, traitSet: {})",
                id,
                sinkName,
                (sinkDescriptor) ? fmt::format("{}", *sinkDescriptor) : "(null)",
                *sinkDescriptor->getSchema(),
                traitSet.explain(verbosity));
        }
        return fmt::format("SINK(opId: {}, sinkName: {})", id, sinkName);
    }
    return fmt::format("SINK({})", sinkName);
}

std::string_view SinkLogicalOperator::getName() const noexcept
{
    return NAME;
}

SinkLogicalOperator SinkLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
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

    if (sinkDescriptor.has_value() && sinkDescriptor.value().isInline() && sinkDescriptor.value().getSchema()->getFields().empty())
    {
        copy.sinkDescriptor->schema = std::make_shared<const Schema>(firstSchema);
    }
    else if (copy.sinkDescriptor.has_value() && *copy.sinkDescriptor->getSchema() != firstSchema)
    {
        std::vector expectedFields(copy.sinkDescriptor.value().getSchema()->begin(), copy.sinkDescriptor.value().getSchema()->end());
        std::vector actualFields(firstSchema.begin(), firstSchema.end());

        std::stringstream expectedFieldsString;
        std::stringstream actualFieldsString;

        for (unsigned int i = 0; i < expectedFields.size(); ++i)
        {
            const auto& field = expectedFields.at(i);
            auto foundIndex = std::ranges::find(actualFields, field);

            if (foundIndex == actualFields.end())
            {
                expectedFieldsString << field << ", ";
            }
            else if (auto foundOffset = foundIndex - std::ranges::begin(actualFields); foundOffset != i)
            {
                expectedFieldsString << fmt::format("Field {} at {}, but was at {},", field, i, foundOffset);
            }
        }
        for (const auto& field : actualFields)
        {
            if (std::ranges::find(expectedFields, field) == expectedFields.end())
            {
                actualFieldsString << field << ", ";
            }
        }

        throw CannotInferSchema(
            "The schema of the sink must be equal to the schema of the input operator. Expected fields {} where not found, and found "
            "unexpected fields {}",
            expectedFieldsString.str(),
            actualFieldsString.str().substr(0, actualFieldsString.str().size() - 2));
    }
    return copy;
}

SinkLogicalOperator SinkLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

TraitSet SinkLogicalOperator::getTraitSet() const
{
    return traitSet;
}

SinkLogicalOperator SinkLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = std::move(children);
    return copy;
}

std::vector<Schema> SinkLogicalOperator::getInputSchemas() const
{
    INVARIANT(!children.empty(), "Sink should have at least one child");
    return children | std::ranges::views::transform([](const LogicalOperator& child) { return child.getOutputSchema(); })
        | std::ranges::to<std::vector>();
};

Schema SinkLogicalOperator::getOutputSchema() const
{
    INVARIANT(this->sinkDescriptor.has_value(), "Logical Sink must have a valid descriptor (with a schema).");
    return *this->sinkDescriptor.value().getSchema();
}

std::vector<LogicalOperator> SinkLogicalOperator::getChildren() const
{
    return children;
}

std::string SinkLogicalOperator::getSinkName() const noexcept
{
    return sinkName;
}

std::optional<SinkDescriptor> SinkLogicalOperator::getSinkDescriptor() const
{
    return sinkDescriptor;
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
SinkLogicalOperator SinkLogicalOperator::withSinkDescriptor(SinkDescriptor sinkDescriptor) const
{
    SinkLogicalOperator newOperator(*this);
    newOperator.sinkDescriptor = std::move(sinkDescriptor);
    return newOperator;
}

Reflected Reflector<SinkLogicalOperator>::operator()(const SinkLogicalOperator& op) const
{
    return reflect(detail::ReflectedSinkLogicalOperator{.sinkDescriptor = op.getSinkDescriptor(), .sinkName = op.getSinkName()});
}

SinkLogicalOperator Unreflector<SinkLogicalOperator>::operator()(const Reflected& reflected) const
{
    auto [descriptor, name] = unreflect<detail::ReflectedSinkLogicalOperator>(reflected);
    if (descriptor.has_value())
    {
        if (descriptor->getSinkName() != name)
        {
            throw CannotDeserialize(
                "SinkLogicalOperator cannot be deserialized because the sink name in the operator ({}) and the sink name in the "
                "SinkDescriptor (do not link to the same sink ({}) are not equal.",
                descriptor->getSinkName(),
                name);
        }

        return SinkLogicalOperator{descriptor.value()};
    }
    return SinkLogicalOperator{name};
}
}
