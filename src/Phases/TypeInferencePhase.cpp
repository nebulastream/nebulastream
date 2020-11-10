/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Catalogs/StreamCatalog.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/OperatorNode.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Plans/Query/QueryPlan.hpp>

namespace NES {

TypeInferencePhase::TypeInferencePhase(StreamCatalogPtr streamCatalog) : streamCatalog(streamCatalog) {
    NES_DEBUG("TypeInferencePhase()");
}

TypeInferencePhase::~TypeInferencePhase() {
    NES_DEBUG("~TypeInferencePhase()");
}

TypeInferencePhasePtr TypeInferencePhase::create(StreamCatalogPtr streamCatalog) {
    return std::make_shared<TypeInferencePhase>(TypeInferencePhase(streamCatalog));
}

QueryPlanPtr TypeInferencePhase::execute(QueryPlanPtr queryPlan) {
    // first we have to check if all source operators have a correct source descriptors
    auto sources = queryPlan->getSourceOperators();

    if (!sources.empty() && !streamCatalog) {
        NES_WARNING("TypeInferencePhase: No StreamCatalog specified!");
    }

    for (auto source : sources) {
        auto sourceDescriptor = source->getSourceDescriptor();

        // if the source descriptor is only a logical stream source we have to replace it with the correct
        // source descriptor form the catalog.
        if (sourceDescriptor->instanceOf<LogicalStreamSourceDescriptor>()) {
            auto streamName = sourceDescriptor->getStreamName();
            SchemaPtr schema;
            try {
               schema = streamCatalog->getSchemaForLogicalStream(streamName);
            }
            catch (std::exception& e) {
                NES_THROW_RUNTIME_ERROR("TypeInferencePhase: The logical stream " + streamName + " could not be found in the StreamCatalog");
            }
            sourceDescriptor->setSchema(schema);
            NES_DEBUG("TypeInferencePhase: update source descriptor for stream " << streamName << " with schema: " << schema->toString());
        }
    }
    // now we have to infer the input and output schemas for the whole query.
    // to this end we call at each sink the infer method to propagate the schemata across the whole query.
    auto sinks = queryPlan->getSinkOperators();
    for (auto sink : sinks) {
        if (!sink->inferSchema()) {
            return nullptr;
        }
    }
    NES_DEBUG("TypeInferencePhase: we inferred all schemas");
    return queryPlan;
}

}// namespace NES