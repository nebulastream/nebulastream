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
#include <memory>
#include <utility>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <SourceCatalogs/SourceCatalog.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES::Optimizer
{

TypeInferencePhase::TypeInferencePhase(std::shared_ptr<Catalogs::Source::SourceCatalog> sourceCatalog)
    : sourceCatalog(std::move(sourceCatalog))
{
}

std::shared_ptr<TypeInferencePhase> TypeInferencePhase::create(std::shared_ptr<Catalogs::Source::SourceCatalog> sourceCatalog)
{
    return std::make_shared<TypeInferencePhase>(TypeInferencePhase(std::move(sourceCatalog)));
}

std::shared_ptr<QueryPlan> TypeInferencePhase::performTypeInferenceQuery(std::shared_ptr<QueryPlan> queryPlan)
{
    /// Infer schema recursively, starting with sinks for sinks.
    auto sinkOperators = queryPlan->getSinkOperators();
    INVARIANT(not sinkOperators.empty(), "Found no sink operators for query plan: {}", queryPlan->getQueryId());

    /// now we have to infer the input and output schemas for the whole query.
    /// to this end we call at each sink the infer method to propagate the schemata across the whole query.
    for (auto& sink : sinkOperators)
    {
        if (!sink->inferSchema())
        {
            throw TypeInferenceException("TypeInferencePhase failed for query with id: {}", queryPlan->getQueryId());
        }
    }
    return queryPlan;
}

void TypeInferencePhase::performTypeInferenceSources(const std::vector<std::shared_ptr<SourceNameLogicalOperator>>& sourceOperators) const
{
    PRECONDITION(sourceCatalog, "Cannot infer types for sources without source catalog.");
    PRECONDITION(not sourceOperators.empty(), "Query plan did not contain sources during type inference.");

    /// first we have to check if all source operators have a correct source descriptors
    for (const auto& source : sourceOperators)
    {
        /// if the source descriptor has no schema set and is only a logical source we replace it with the correct
        /// source descriptor form the catalog.
        auto logicalSourceName = source->getLogicalSourceName();
        std::shared_ptr<Schema> schema = Schema::create();
        if (!sourceCatalog->containsLogicalSource(logicalSourceName))
        {
            NES_ERROR("Source name: {} not registered.", logicalSourceName);
            throw LogicalSourceNotFoundInQueryDescription(fmt::format("Logical source not registered. Source Name: {}", logicalSourceName));
        }
        auto originalSchema = sourceCatalog->getSchemaForLogicalSource(logicalSourceName);
        schema = schema->copyFields(originalSchema);
        schema->setLayoutType(originalSchema->getLayoutType());
        auto qualifierName = logicalSourceName + Schema::ATTRIBUTE_NAME_SEPARATOR;
        /// perform attribute name resolution
        for (const auto& field : *schema)
        {
            if (!field->getName().starts_with(qualifierName))
            {
                field->setName(qualifierName + field->getName());
            }
        }
        source->setSchema(schema);
        NES_DEBUG("TypeInferencePhase: update source descriptor for source {} with schema: {}", logicalSourceName, schema->toString());
    }
}

}
