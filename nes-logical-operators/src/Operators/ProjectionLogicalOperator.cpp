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

#include <Operators/ProjectionLogicalOperator.hpp>

#include <cstddef>
#include <numeric>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <Configurations/Descriptor.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Traits/Trait.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

namespace
{
std::string explainProjection(const ProjectionLogicalOperator::Projection& projection, ExplainVerbosity verbosity)
{
    std::stringstream builder;
    builder << projection.second.explain(verbosity);
    if (projection.first)
    {
        builder << " as " << projection.first->getFieldName();
    }
    return builder.str();
}
}

ProjectionLogicalOperator::ProjectionLogicalOperator(std::vector<Projection> projections, Asterisk asterisk)
    : data(std::move(projections), asterisk.value)
{
}

std::string_view ProjectionLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::vector<std::string> ProjectionLogicalOperator::getAccessedFields() const
{
    if (data.asterisk)
    {
        return inputSchemas.front().getFieldNames();
    }

    return data.projections | std::views::values
        | std::views::transform(
               [](const LogicalFunction& function)
               {
                   return BFSRange(function)
                       | std::views::filter([](const LogicalFunction& function)
                                            { return function.tryGet<FieldAccessLogicalFunction>().has_value(); })
                       | std::views::transform([](const LogicalFunction& function)
                                               { return function.get<FieldAccessLogicalFunction>().getFieldName(); });
               })
        | std::views::join | std::ranges::to<std::vector>();
}

const std::vector<ProjectionLogicalOperator::Projection>& ProjectionLogicalOperator::getProjections() const
{
    return data.projections;
}

std::string ProjectionLogicalOperator::explain(ExplainVerbosity verbosity) const
{
    auto explainedProjections
        = std::views::transform(data.projections, [&](const auto& projection) { return explainProjection(projection, verbosity); });
    std::stringstream builder;
    builder << "PROJECTION(";
    if (verbosity == ExplainVerbosity::Debug)
    {
        builder << "opId: " << id << ", ";
    }
    if (!outputSchema.getFieldNames().empty())
    {
        builder << "schema: " << outputSchema << ", ";
    }

    builder << "fields: [";
    if (data.asterisk)
    {
        builder << "*";
        if (!data.projections.empty())
        {
            builder << ", ";
        }
    }
    builder << fmt::format("{}", fmt::join(explainedProjections, ", "));
    builder << "])";
    return builder.str();
}

LogicalOperator ProjectionLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    INVARIANT(!inputSchemas.empty(), "Projection should have at least one input");

    const auto& firstSchema = inputSchemas[0];
    for (const auto& schema : inputSchemas)
    {
        if (schema != firstSchema)
        {
            throw CannotInferSchema("All input schemas must be equal for Projection operator");
        }
    }

    /// Propagate the type inference to all projection functions and resolve projection names.
    auto inferredProjections = data.projections
        | std::views::transform(
                                   [&firstSchema](const Projection& projection)
                                   {
                                       auto inferredFunction = projection.second.withInferredDataType(firstSchema);
                                       /// If projection has a name use it.
                                       if (projection.first)
                                       {
                                           return Projection{*projection.first, inferredFunction};
                                       }
                                       /// Otherwise derive the name from the inferred function
                                       return Projection{inferredFunction.explain(ExplainVerbosity::Short), inferredFunction};
                                   });

    auto copy = *this;
    copy.data.projections = inferredProjections | std::ranges::to<std::vector>();
    copy.inputSchemas = inputSchemas;

    /// Resolve the output schema of the Projection. If an asterisk is used we propagate the entire input schema
    auto initial = Schema{copy.outputSchema.memoryLayoutType};
    if (data.asterisk)
    {
        initial.appendFieldsFromOtherSchema(copy.inputSchemas.front());
    }
    copy.outputSchema = std::accumulate(
        inferredProjections.begin(),
        inferredProjections.end(),
        initial,
        [](Schema schema, const auto& projection)
        { return schema.addField(projection.first->getFieldName(), projection.second.getDataType()); });

    return copy;
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterProjectionLogicalOperator(NES::LogicalOperatorRegistryArguments)
{
    return ProjectionLogicalOperator({}, ProjectionLogicalOperator::Asterisk{false});
}

}
