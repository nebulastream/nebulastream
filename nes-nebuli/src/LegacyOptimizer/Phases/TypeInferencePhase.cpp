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

void apply(std::vector<SourceNameLogicalOperator>& sourceOperators,  Catalogs::Source::SourceCatalog& sourceCatalog)
{
    PRECONDITION(not sourceOperators.empty(), "Query plan did not contain sources during type inference.");

    /// first we have to check if all source operators have a correct source descriptors
    for (auto& source : sourceOperators)
    {
        /// if the source descriptor has no schema set and is only a logical source we replace it with the correct
        /// source descriptor form the catalog.
        auto logicalSourceName = source.getName();
        Schema schema = Schema();
        if (!sourceCatalog.containsLogicalSource(std::string(logicalSourceName)))
        {
            NES_ERROR("Source name: {} not registered.", logicalSourceName);
            throw LogicalSourceNotFoundInQueryDescription(fmt::format("Logical source not registered. Source Name: {}", logicalSourceName));
        }
        auto originalSchema = sourceCatalog.getSchemaForLogicalSource(std::string(logicalSourceName));
        schema = schema.copyFields(originalSchema);
        schema.setLayoutType(originalSchema.getLayoutType());
        auto qualifierName = std::string(logicalSourceName) + Schema::ATTRIBUTE_NAME_SEPARATOR;
        /// perform attribute name resolution
        for (auto& field : schema)
        {
            if (!field.getName().starts_with(qualifierName))
            {
                field.setName(qualifierName + field.getName());
            }
        }
        source.setSchema(schema);
        NES_DEBUG("TypeInferencePhase: update source descriptor for source {} with schema: {}", logicalSourceName, schema.toString());
    }
}

void TypeInferencePhase::apply(QueryPlan& queryPlan)
{
    auto sourceOperators = queryPlan.getOperatorByType<SourceDescriptorLogicalOperator>();
    INVARIANT(not sourceOperators.empty(), "Found no source operators for query plan: {}", queryPlan.getQueryId());

    /// now we have to infer the input and output schemas for the whole query.
    /// to this end we call at each sink the infer method to propagate the schemata across the whole query.
    for (auto& source : sourceOperators)
    {
        if (!source.inferSchema())
        {
            throw TypeInferenceException("TypeInferencePhase failed for query with id: {}", queryPlan.getQueryId());
        }
    }
}



}
