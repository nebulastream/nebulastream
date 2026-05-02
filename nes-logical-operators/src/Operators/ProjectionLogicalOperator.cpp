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

#include <optional>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/LogicalType.hpp>
#include <DataTypes/LogicalTypeBridge.hpp>
#include <DataTypes/PhysicalLayout.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Traits/Trait.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <LogicalTypeLoweringRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

namespace
{
std::string explainProjection(const ProjectionLogicalOperator::Projection& projection, ExplainVerbosity verbosity)
{
    std::stringstream builder;
    builder << projection.second.explain(verbosity);
    if (projection.first && projection.first->getFieldName() != projection.second.explain(verbosity))
    {
        builder << " as " << projection.first->getFieldName();
    }
    return builder.str();
}

/// Look up the physical layout for a logical type. Compound types resolve to
/// multiple primitive components; primitive types fall back to a single
/// component lowered via the `toPhysical` bridge.
///
/// TODO(datatype-migration, phase-7): the flattening of compound LogicalType
/// fields belongs at the lowering boundary (when we generate the physical
/// runtime schema), not at logical-projection time. For the prototype this
/// stays here so that systests that declare flat sink schemas keep working.
PhysicalLayout resolveLayout(const LogicalType& logicalType)
{
    if (auto layoutOpt = LogicalTypeLoweringRegistry::instance().create(
            std::string(logicalType.getName()), LogicalTypeLoweringRegistryArguments{.logicalType = logicalType}))
    {
        return std::move(layoutOpt).value();
    }
    if (const auto physical = toPhysical(logicalType); physical.has_value())
    {
        return PhysicalLayout{.components = {{.suffix = "", .physicalType = *physical}}};
    }
    INVARIANT(
        false,
        "No physical layout registered for logical type '{}'. Add an entry to LogicalTypeLoweringRegistry.",
        logicalType.getName());
    std::unreachable();
}

/// Map a primitive physical DataType back into the prototype's reduced
/// LogicalType vocabulary. The flattened component types must use the
/// user-facing names ("INTEGER", "FLOAT", "BOOL", "TEXT"), not the magic-enum
/// names of the underlying DataType::Type ("INT64", "FLOAT64", ...).
LogicalType prototypeLogicalTypeFromPhysical(const DataType& physicalType)
{
    const auto nullable = physicalType.nullable ? Nullable::IS_NULLABLE : Nullable::NOT_NULLABLE;
    using Type = DataType::Type;
    switch (physicalType.type)
    {
        case Type::INT8:
        case Type::INT16:
        case Type::INT32:
        case Type::INT64:
        case Type::UINT8:
        case Type::UINT16:
        case Type::UINT32:
        case Type::UINT64:
            return LogicalType{"INTEGER", {}, nullable};
        case Type::FLOAT32:
        case Type::FLOAT64:
            return LogicalType{"FLOAT", {}, nullable};
        case Type::BOOLEAN:
            return LogicalType{"BOOL", {}, nullable};
        case Type::CHAR:
        case Type::VARSIZED:
            return LogicalType{"TEXT", {}, nullable};
        case Type::UNDEFINED:
            return LogicalType{"UNDEFINED", {}, nullable};
    }
    return LogicalType{"UNDEFINED", {}, nullable};
}
}

ProjectionLogicalOperator::ProjectionLogicalOperator(std::vector<Projection> projections, Asterisk asterisk)
    : projections(std::move(projections)), asterisk(asterisk.value)
{
}

std::string_view ProjectionLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::vector<std::string> ProjectionLogicalOperator::getAccessedFields() const
{
    if (asterisk)
    {
        return inputSchema.getFieldNames();
    }

    return projections | std::views::values
        | std::views::transform(
               [](const LogicalFunction& function)
               {
                   return BFSRange(function)
                       | std::views::filter([](const LogicalFunction& function)
                                            { return function.tryGetAs<FieldAccessLogicalFunction>().has_value(); })
                       | std::views::transform([](const LogicalFunction& function)
                                               { return function.getAs<FieldAccessLogicalFunction>()->getFieldName(); });
               })
        | std::views::join | std::ranges::to<std::vector>();
}

const std::vector<ProjectionLogicalOperator::Projection>& ProjectionLogicalOperator::getProjections() const
{
    return projections;
}

bool ProjectionLogicalOperator::operator==(const ProjectionLogicalOperator& rhs) const
{
    return projections == rhs.projections && outputSchema == rhs.outputSchema && inputSchema == rhs.inputSchema
        && getTraitSet() == rhs.getTraitSet();
};

std::string ProjectionLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    auto explainedProjections
        = std::views::transform(projections, [&](const auto& projection) { return explainProjection(projection, verbosity); });
    std::stringstream builder;
    builder << "PROJECTION(";
    if (verbosity == ExplainVerbosity::Debug)
    {
        builder << "opId: " << id << ", ";
        if (!outputSchema.getFieldNames().empty())
        {
            builder << "schema: " << outputSchema << ", ";
        }
    }

    builder << "fields: [";
    if (asterisk)
    {
        builder << "*";
        if (!projections.empty())
        {
            builder << ", ";
        }
    }
    builder << fmt::format("{}", fmt::join(explainedProjections, ", "));
    builder << "]";

    if (verbosity == ExplainVerbosity::Debug)
    {
        builder << fmt::format(", traitSet: {}", traitSet.explain(verbosity));
    }
    builder << ")";
    return builder.str();
}

ProjectionLogicalOperator ProjectionLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    if (inputSchemas.empty())
    {
        throw CannotDeserialize("Projection should have at least one input");
    }

    const auto& firstSchema = inputSchemas[0];
    for (const auto& schema : inputSchemas)
    {
        if (schema != firstSchema)
        {
            throw CannotInferSchema("All input schemas must be equal for Projection operator");
        }
    }

    /// Type-infer projection functions, resolve names, reject duplicate AS aliases.
    /// An alias is explicit when its name differs from the auto-derived name (function.explain(Short)).
    auto copy = *this;
    copy.projections.clear();
    copy.inputSchema = firstSchema;
    std::unordered_set<std::string> seenExplicitAliases;
    for (const auto& projection : projections)
    {
        auto inferredFunction = projection.second.withInferredLogicalType(firstSchema);
        const auto autoDerivedName = inferredFunction.explain(ExplainVerbosity::Short);
        const bool hasExplicitAlias = projection.first.has_value() && projection.first->getFieldName() != autoDerivedName;
        auto identifier = hasExplicitAlias ? *projection.first : FieldIdentifier(autoDerivedName);
        if (hasExplicitAlias && !seenExplicitAliases.insert(identifier.getFieldName()).second)
        {
            throw CannotInferSchema(
                "Duplicate output field name '{}' in SELECT list. Use AS to give each column a unique name.", identifier.getFieldName());
        }
        copy.projections.emplace_back(std::move(identifier), std::move(inferredFunction));
    }

    /// Resolve the output schema. Each projected expression contributes one or
    /// more fields, depending on its result LogicalType's PhysicalLayout:
    /// scalar types lower to one field with empty suffix (so the field name is
    /// unchanged); compound types lower to N fields with suffix names (Point
    /// `p` -> `p_X`, `p_Y`, `p_Z`).
    Schema resolved;
    if (asterisk)
    {
        resolved.appendFieldsFromOtherSchema(copy.inputSchema);
    }
    for (const auto& [identifier, function] : copy.projections)
    {
        INVARIANT(identifier.has_value(), "Projection identifier must be set after inference");
        const auto layout = resolveLayout(function.getLogicalType());
        for (const auto& component : layout.components)
        {
            resolved.addField(identifier->getFieldName() + component.suffix, prototypeLogicalTypeFromPhysical(component.physicalType));
        }
    }
    copy.outputSchema = std::move(resolved);
    return copy;
}

TraitSet ProjectionLogicalOperator::getTraitSet() const
{
    return traitSet;
}

ProjectionLogicalOperator ProjectionLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

ProjectionLogicalOperator ProjectionLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = std::move(children);
    return copy;
}

std::vector<Schema> ProjectionLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
}

Schema ProjectionLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<LogicalOperator> ProjectionLogicalOperator::getChildren() const
{
    return children;
}

Reflected Reflector<ProjectionLogicalOperator>::operator()(const ProjectionLogicalOperator& op) const
{
    detail::ReflectedProjectionLogicalOperator reflected;

    for (auto [identifierOpt, function] : op.getProjections())
    {
        /// Only serialize identifiers that differ from the auto-derived name (i.e. explicit AS aliases).
        std::optional<std::string> identifier;
        if (identifierOpt.has_value())
        {
            const auto autoDerived = function.explain(ExplainVerbosity::Short);
            if (identifierOpt->getFieldName() != autoDerived)
            {
                identifier = identifierOpt->getFieldName();
            }
        }
        reflected.projections.emplace_back(identifier, std::make_optional(function));
    }

    reflected.asterisk = op.asterisk;

    return reflect(reflected);
}

ProjectionLogicalOperator Unreflector<ProjectionLogicalOperator>::operator()(const Reflected& reflected) const
{
    auto [asterisk, projections] = unreflect<detail::ReflectedProjectionLogicalOperator>(reflected);

    std::vector<ProjectionLogicalOperator::Projection> parsedProjections;

    for (auto [identifier, function] : projections)
    {
        if (!function.has_value())
        {
            throw CannotDeserialize("Failed to deserialize projection function");
        }

        parsedProjections.emplace_back(identifier, function.value());
    }

    return {std::move(parsedProjections), ProjectionLogicalOperator::Asterisk(asterisk)};
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterProjectionLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<ProjectionLogicalOperator>(arguments.reflected);
    }

    PRECONDITION(false, "Operator is only build directly via parser or via reflection, not using the registry");
    std::unreachable();
}

}
