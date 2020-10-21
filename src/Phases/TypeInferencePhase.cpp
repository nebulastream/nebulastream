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
    for (auto source : sources) {
        auto sourceDescriptor = source->getSourceDescriptor();

        // if the source descriptor is only a logical stream source we have to replace it with the correct
        // source descriptor form the catalog.
        if (sourceDescriptor->instanceOf<LogicalStreamSourceDescriptor>()) {
            auto streamName = sourceDescriptor->getStreamName();
            SchemaPtr schema = streamCatalog->getSchemaForLogicalStream(streamName);
            sourceDescriptor->setSchema(schema);
            NES_DEBUG("TypeInferencePhase: update source descriptor for stream " << streamName);
        }
    }
    // now we have to infer the input and output schemas for the whole query.
    // to this end we call at each sink the infer method to propagate the schemata across the whole query.
    auto sinks = queryPlan->getSinkOperators();
    for (auto sink : sinks) {
        sink->inferSchema();
    }
    NES_DEBUG("TypeInferencePhase: we inferred all schemas");
    return queryPlan;
}

}// namespace NES