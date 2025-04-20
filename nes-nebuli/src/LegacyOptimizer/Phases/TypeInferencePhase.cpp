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
#include <LegacyOptimizer/Phases/TypeInferencePhase.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <SourceCatalogs/SourceCatalog.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <Plans/LogicalPlan.hpp>

namespace NES::LegacyOptimizer
{

void TypeInferencePhase::apply(LogicalPlan& queryPlan,  Catalogs::Source::SourceCatalog& sourceCatalog)
{
    auto sourceOperators = queryPlan.getOperatorByType<SourceNameLogicalOperator>();

    PRECONDITION(not sourceOperators.empty(), "Query plan did not contain sources during type inference.");

    std::vector<LogicalOperator> newSources;
    /// first we have to check if all source operators have a correct source descriptors
    for (auto& source : sourceOperators)
    {
        /// if the source descriptor has no schema set and is only a logical source we replace it with the correct
        /// source descriptor form the catalog.
        auto schema = Schema();
        if (!sourceCatalog.containsLogicalSource(source.getLogicalSourceName()))
        {
            NES_ERROR("Source name: {} not registered.", source.getLogicalSourceName());
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
        source.setSchema(schema);
        newSources.push_back(source);
    }
    queryPlan.rootOperators = newSources;
}

void TypeInferencePhase::apply(QueryPlan& queryPlan)
{
    auto sourceOperators = queryPlan.getOperatorByType<SourceDescriptorLogicalOperator>();
    INVARIANT(not sourceOperators.empty(), "Found no source operators for query plan: {}", queryPlan.getQueryId());

    /// now we have to infer the input and output schemas for the whole query.
    /// to this end we call at each sink the infer method to propagate the schemata across the whole query.
    std::vector<LogicalOperator> newSources;
    for (auto& source : sourceOperators)
    {
        newSources.push_back(source.withInferredSchema(Schema()));
    }
    queryPlan.rootOperators = newSources;
}



}
