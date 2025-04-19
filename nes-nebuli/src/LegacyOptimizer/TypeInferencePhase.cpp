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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <LegacyOptimizer/TypeInferencePhase.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <SourceCatalogs/SourceCatalog.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES::LegacyOptimizer
{

void TypeInferencePhase::apply(LogicalPlan& queryPlan, Catalogs::Source::SourceCatalog& sourceCatalog)
{
    auto sourceOperators = queryPlan.getOperatorByType<SourceNameLogicalOperator>();

    PRECONDITION(not sourceOperators.empty(), "Query plan did not contain sources during type inference.");

    for (auto& source : sourceOperators)
    {
        /// if the source descriptor has no schema set and is only a logical source we replace it with the correct
        /// source descriptor form the catalog.
        auto schema = Schema();
        if (!sourceCatalog.containsLogicalSource(source.getLogicalSourceName()))
        {
            throw LogicalSourceNotFoundInQueryDescription("Logical source not registered. Source Name: {}", source.getLogicalSourceName());
        }
        auto originalSchema = sourceCatalog.getSchemaForLogicalSource(source.getLogicalSourceName());
        schema = schema.copyFields(originalSchema);
        schema.setLayoutType(originalSchema.getLayoutType());
        auto qualifierName = source.getLogicalSourceName() + Schema::ATTRIBUTE_NAME_SEPARATOR;
        /// perform attribute name resolution
        for (auto& field : schema)
        {
            if (!field.getName().starts_with(qualifierName))
            {
                field.setName(qualifierName + field.getName());
            }
        }
        queryPlan.replaceOperator(source, source.withSchema(schema));
    }
}

LogicalOperator propagateSchema(const LogicalOperator& op)
{
    std::vector<LogicalOperator> children = op.getChildren();

    // Base case: if no children (source operators)
    if (children.empty())
    {
        return op;
    }

    std::vector<LogicalOperator> newChildren;
    std::vector<Schema> childSchemas;
    for (const auto& child : children)
    {
        LogicalOperator childWithSchema = propagateSchema(child);
        childSchemas.push_back(childWithSchema.getOutputSchema());
        newChildren.push_back(childWithSchema);
    }

    LogicalOperator updatedOperator = op.withChildren(newChildren);
    return updatedOperator.withInferredSchema(childSchemas);
}

void TypeInferencePhase::apply(QueryPlan& queryPlan)
{
    std::vector<LogicalOperator> newRoots;
    for (const auto& sink : queryPlan.rootOperators)
    {
        LogicalOperator inferredRoot = propagateSchema(sink);
        newRoots.push_back(inferredRoot);
    }
    queryPlan.rootOperators = newRoots;
}


}
