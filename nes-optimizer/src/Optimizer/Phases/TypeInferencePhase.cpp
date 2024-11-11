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
#include <Catalogs/Exceptions/LogicalSourceNotFoundException.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Operators/Exceptions/TypeInferenceException.hpp>
#include <Operators/LogicalOperators/LogicalFilterOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES::Optimizer {

TypeInferencePhase::TypeInferencePhase(Catalogs::Source::SourceCatalogPtr sourceCatalog, Catalogs::UDF::UDFCatalogPtr udfCatalog)
    : sourceCatalog(std::move(sourceCatalog)), udfCatalog(std::move(udfCatalog)) {
    NES_DEBUG("TypeInferencePhase()");
}

TypeInferencePhasePtr TypeInferencePhase::create(Catalogs::Source::SourceCatalogPtr sourceCatalog,
                                                 Catalogs::UDF::UDFCatalogPtr udfCatalog) {
    return std::make_shared<TypeInferencePhase>(TypeInferencePhase(std::move(sourceCatalog), std::move(udfCatalog)));
}

QueryPlanPtr TypeInferencePhase::execute(QueryPlanPtr queryPlan) {

    if (!sourceCatalog) {
        NES_WARNING("TypeInferencePhase: No SourceCatalog specified!");
    }
    // Fetch the source and sink operators.
    auto sourceOperators = queryPlan->getSourceOperators();
    auto sinkOperators = queryPlan->getSinkOperators();

    if (sourceOperators.empty() || sinkOperators.empty()) {
        throw TypeInferenceException(queryPlan->getQueryId(), "Found no source or sink operators");
    }

    performTypeInference(queryPlan->getQueryId(), sourceOperators, sinkOperators);
    NES_DEBUG("TypeInferencePhase: we inferred all schemas");
    return queryPlan;
}

DecomposedQueryPlanPtr TypeInferencePhase::execute(DecomposedQueryPlanPtr decomposedQueryPlan, FaultToleranceType faultToleranceType) {

    if (!sourceCatalog) {
        NES_WARNING("TypeInferencePhase: No SourceCatalog specified!");
    }
    // Fetch the source and sink operators.
    auto sourceOperators = decomposedQueryPlan->getSourceOperators();
    auto sinkOperators = decomposedQueryPlan->getSinkOperators();

    if (sourceOperators.empty() || sinkOperators.empty()) {
        throw TypeInferenceException(UNSURE_CONVERSION_TODO_4761(decomposedQueryPlan->getDecomposedQueryId(), QueryId),
                                     "Found no source or sink operators");
    }

    performTypeInference(UNSURE_CONVERSION_TODO_4761(decomposedQueryPlan->getDecomposedQueryId(), QueryId, faultToleranceType),
                         sourceOperators,
                         sinkOperators);
    NES_DEBUG("TypeInferencePhase: we inferred all schemas");
    return decomposedQueryPlan;
}

void TypeInferencePhase::performTypeInference(QueryId planId,
                                              std::vector<SourceLogicalOperatorPtr> sourceOperators,
                                              std::vector<SinkLogicalOperatorPtr> sinkOperators,
                                              FaultToleranceType faultToleranceType) {

    // first we have to check if all source operators have a correct source descriptors
    for (const auto& source : sourceOperators) {
        auto sourceDescriptor = source->getSourceDescriptor();

        // if the source descriptor has no schema set and is only a logical source we replace it with the correct
        // source descriptor form the catalog.
        if (sourceDescriptor->instanceOf<LogicalSourceDescriptor>() && sourceDescriptor->getSchema()->empty()) {
            auto logicalSourceName = sourceDescriptor->getLogicalSourceName();
            SchemaPtr schema = Schema::create();
            if (!sourceCatalog->containsLogicalSource(logicalSourceName)) {
                NES_ERROR("Source name: {} not registered.", logicalSourceName);
                throw Exceptions::LogicalSourceNotFoundException("Logical source not registered. Source Name: "
                                                                 + logicalSourceName);
            }
            auto originalSchema = sourceCatalog->getSchemaForLogicalSource(logicalSourceName);
            schema = schema->copyFields(originalSchema);
            schema->setLayoutType(originalSchema->getLayoutType());
            std::string qualifierName = logicalSourceName + Schema::ATTRIBUTE_NAME_SEPARATOR;
            //perform attribute name resolution
            for (auto& field : schema->fields) {
                if (!field->getName().starts_with(qualifierName)) {
                    field->setName(qualifierName + field->getName());
                }
            }
            sourceDescriptor->setSchema(schema);
            NES_DEBUG("TypeInferencePhase: update source descriptor for source {} with schema: {}",
                      logicalSourceName,
                      schema->toString());
        }
    }

    // now we have to infer the input and output schemas for the whole query.
    // to this end we call at each sink the infer method to propagate the schemata across the whole query.
    for (auto& sink : sinkOperators) {
        auto sinkDescriptor = sink->getSinkDescriptor()->as<SinkDescriptor>();
        sinkDescriptor->setFaultToleranceType(faultToleranceType);
        sink->setSinkDescriptor(sinkDescriptor);
        if (!sink->inferSchema()) {
            NES_ERROR("TypeInferencePhase: Exception occurred during type inference phase.");
            throw TypeInferenceException(planId, "TypeInferencePhase: Failed!");
        }
    }
}

}// namespace NES::Optimizer
