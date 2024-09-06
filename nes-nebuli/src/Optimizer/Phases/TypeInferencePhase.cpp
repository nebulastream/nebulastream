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
#include <utility>
#include <API/AttributeField.hpp>
#include <Operators/Exceptions/TypeInferenceException.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/OperatorLogicalSourceDescriptor.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <SourceCatalogs/SourceCatalog.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES::Optimizer
{

TypeInferencePhase::TypeInferencePhase(Catalogs::Source::SourceCatalogPtr sourceCatalog)
    : isFirstExecuteCall(true), sourceCatalog(std::move(sourceCatalog))
{
}

TypeInferencePhasePtr TypeInferencePhase::create(Catalogs::Source::SourceCatalogPtr sourceCatalog)
{
    return std::make_shared<TypeInferencePhase>(TypeInferencePhase(std::move(sourceCatalog)));
}

QueryPlanPtr TypeInferencePhase::execute(QueryPlanPtr queryPlan)
{
    if (!sourceCatalog)
    {
        NES_WARNING("TypeInferencePhase: No SourceCatalog specified!");
    }

    /// We only need to infere the schema for sources once, before specifying the source.
    if (isFirstExecuteCall)
    {
        auto sourceOperators = queryPlan->getSourceOperators<SourceLogicalOperator>();
        if (sourceOperators.empty())
        {
            throw TypeInferenceException(queryPlan->getQueryId(), "Found no source or sink operators");
        }
        performTypeInferenceSources(sourceOperators);
        isFirstExecuteCall = false;
    };

    /// Infer schema for sinks.
    auto sinkOperators = queryPlan->getSinkOperators();
    if (sinkOperators.empty())
    {
        throw TypeInferenceException(queryPlan->getQueryId(), "Found no source or sink operators");
    }
    performTypeInferenceSinks(queryPlan->getQueryId(), sinkOperators);

    NES_DEBUG("TypeInferencePhase: we inferred all schemas");
    return queryPlan;
}

void TypeInferencePhase::performTypeInferenceSinks(QueryId queryId, const std::vector<SinkLogicalOperatorPtr>& sinkOperators)
{
    /// now we have to infer the input and output schemas for the whole query.
    /// to this end we call at each sink the infer method to propagate the schemata across the whole query.
    for (auto& sink : sinkOperators)
    {
        if (!sink->inferSchema())
        {
            NES_ERROR("TypeInferencePhase: Exception occurred during type inference phase.");
            throw TypeInferenceException(queryId, "TypeInferencePhase: Failed!");
        }
    }
}


void TypeInferencePhase::performTypeInferenceSources(const std::vector<std::shared_ptr<SourceLogicalOperator>>& sourceOperators)
{
    /// first we have to check if all source operators have a correct source descriptors
    for (const auto& source : sourceOperators)
    {
        INVARIANT(!source->getSchema(), "A ")
        /// if the source descriptor has no schema set and is only a logical source we replace it with the correct
        /// source descriptor form the catalog.
        ///-Todo: improve
        auto logicalSourceName = source->getLogicalSourceName();
        SchemaPtr schema = Schema::create();
        if (!sourceCatalog->containsLogicalSource(logicalSourceName))
        {
            NES_ERROR("Source name: {} not registered.", logicalSourceName);
            auto ex = LogicalSourceNotFoundInQueryDescription("Logical source not registered. Source Name: " + logicalSourceName);
            throw ex;
        }
        auto originalSchema = sourceCatalog->getSchemaForLogicalSource(logicalSourceName);
        schema = schema->copyFields(originalSchema);
        schema->setLayoutType(originalSchema->getLayoutType());
        std::string qualifierName = logicalSourceName + Schema::ATTRIBUTE_NAME_SEPARATOR;
        /// perform attribute name resolution
        for (auto& field : schema->fields)
        {
            if (!field->getName().starts_with(qualifierName))
            {
                field->setName(qualifierName + field->getName());
            }
        }
        /// Todo: is not persisted (get returns copy)
        source->setSchema(schema);
        NES_DEBUG("TypeInferencePhase: update source descriptor for source {} with schema: {}", logicalSourceName, schema->toString());
    }
}

} /// namespace NES::Optimizer
