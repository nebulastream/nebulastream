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

#include <Rules/Semantic/TypeInferenceRule.hpp>

#include <set>
#include <string>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <utility>
#include <vector>

#include <DataTypes/LogicalSchema.hpp>
#include <DataTypes/LogicalType.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Phases/LogicalTypeLowering/PhysicalLayout.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Semantic/InlineSinkBindingRule.hpp>
#include <Rules/Semantic/LogicalSourceExpansionRule.hpp>
#include <Rules/Semantic/SinkBindingRule.hpp>
#include <ErrorHandling.hpp>
#include <LogicalTypeLoweringRegistry.hpp>

namespace NES
{

namespace
{
PhysicalLayout resolveLayout(const LogicalType& logicalType)
{
    auto layoutOpt = LogicalTypeLoweringRegistry::instance().create(
        std::string(logicalType.getName()), LogicalTypeLoweringRegistryArguments{.logicalType = logicalType});
    INVARIANT(
        layoutOpt.has_value(),
        "No physical layout registered for logical type '{}'. Add an entry to LogicalTypeLoweringRegistry.",
        logicalType.getName());
    return std::move(layoutOpt).value();
}

/// Expand any compound LogicalType in `schema` into N primitive components,
/// using the LogicalTypeLoweringRegistry.
LogicalSchema flattenCompoundFields(const LogicalSchema& schema)
{
    LogicalSchema flattened;
    for (const auto& field : schema.getFields())
    {
        const auto layout = resolveLayout(field.logicalType);
        for (const auto& component : layout.components)
        {
            flattened.addField(field.name + component.suffix, LogicalType::fromPhysical(component.physicalType));
        }
    }
    return flattened;
}
}

static LogicalOperator propagateSchema(const LogicalOperator& op)
{
    const std::vector<LogicalOperator> children = op.getChildren();

    if (children.empty())
    {
        return op;
    }

    std::vector<LogicalOperator> newChildren;
    std::vector<LogicalSchema> childSchemas;
    for (const auto& child : children)
    {
        const LogicalOperator childWithSchema = propagateSchema(child);
        childSchemas.push_back(childWithSchema.getOutputLogicalSchema());
        newChildren.push_back(childWithSchema);
    }

    const LogicalOperator updatedOperator = op.withChildren(newChildren);
    auto inferred = updatedOperator.withInferredLogicalSchema(childSchemas);

    /// ProjectionLogicalOperator is the only operator today whose output may
    /// carry a compound LogicalType (e.g. `Point(x, y, z) AS p`). Flatten the
    /// stored output schema to primitive components so legacy operators
    /// downstream (Sink, etc.) only ever see primitive `LogicalSchema`s — their
    /// `withInferredSchema` bridge would otherwise trip in `toPrimitiveSchema`.
    /// The function tree itself (the projection list) is preserved; the
    /// compound→primitive spread happens at runtime in `Record::write`.
    if (auto projection = inferred.tryGetAs<ProjectionLogicalOperator>())
    {
        const auto flattened = flattenCompoundFields((*projection)->getOutputLogicalSchema());
        return LogicalOperator((*projection)->withOutputLogicalSchema(flattened));
    }
    return inferred;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan TypeInferenceRule::apply(const LogicalPlan& queryPlan) const
{
    std::vector<LogicalOperator> newRoots;
    for (const auto& sink : queryPlan.getRootOperators())
    {
        const LogicalOperator inferredRoot = propagateSchema(sink);
        newRoots.push_back(inferredRoot);
    }
    return queryPlan.withRootOperators(newRoots);
}

const std::type_info& TypeInferenceRule::getType()
{
    return typeid(TypeInferenceRule);
}

std::string_view TypeInferenceRule::getName()
{
    return NAME;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> TypeInferenceRule::dependsOn() const
{
    return {typeid(LogicalSourceExpansionRule), typeid(SinkBindingRule), typeid(InlineSinkBindingRule)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> TypeInferenceRule::requiredBy() const
{
    return {};
}

bool TypeInferenceRule::operator==(const TypeInferenceRule&) const
{
    return true;
}


}
