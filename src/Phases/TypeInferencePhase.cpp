#include <Catalogs/StreamCatalog.hpp>
#include <Nodes/Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/OperatorNode.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Nodes/Util/Iterators/BreadthFirstNodeIterator.hpp>
#include <Plans/Query/QueryPlan.hpp>

namespace NES {

TypeInferencePhase::TypeInferencePhase() {}

TypeInferencePhasePtr TypeInferencePhase::create() {
    return std::make_shared<TypeInferencePhase>(TypeInferencePhase());
}

void TypeInferencePhase::setStreamCatalog(StreamCatalogPtr streamCatalog) {
    this->streamCatalog = streamCatalog;
}

QueryPlanPtr TypeInferencePhase::transform(QueryPlanPtr queryPlan) {
    // first we have to check if all source operators have a correct source descriptors
    auto sources = queryPlan->getSourceOperators();
    for (auto source : sources) {
        auto sourceDescriptor = source->getSourceDescriptor();
        // if the source descriptor is only a logical stream source we have to replace it with the correct
        // source descriptor form the catalog.
        if (sourceDescriptor->instanceOf<LogicalStreamSourceDescriptor>()) {
            auto streamDescriptor = sourceDescriptor->as<LogicalStreamSourceDescriptor>();
            auto streamName = streamDescriptor->getStreamName();
            auto newSourceDescriptor = createSourceDescriptor(streamName);
            source->setSourceDescriptor(newSourceDescriptor);
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

SourceDescriptorPtr TypeInferencePhase::createSourceDescriptor(std::string streamName) {

    auto schema = streamCatalog->getSchemaForLogicalStream(streamName);
    auto physicalStreams = streamCatalog->getPhysicalStreams(streamName);

    if (physicalStreams.empty()) {
        NES_ERROR("TypeInferencePhase: the logical stream does not exists in the catalog " << streamName);
        throw std::invalid_argument("Non existing logical stream : " + streamName + " provided");
    }

    // Pick the first element from the catalog entry and identify the type to create appropriate source type
    // todo add handling for support of multiple physical streams.
    auto physicalStream = physicalStreams[0];
    std::string name = physicalStream->getPhysicalName();
    std::string type = physicalStream->getSourceType();
    std::string conf = physicalStream->getSourceConfig();
    size_t frequency = physicalStream->getSourceFrequency();
    size_t numBuffers = physicalStream->getNumberOfBuffersToProduce();

    NES_DEBUG("TypeInferencePhase: logical stream name=" << streamName << " pyhName=" << name
                                                         << " srcType=" << type << " srcConf=" << conf
                                                         << " frequency=" << frequency << " numBuffers=" << numBuffers);

    if (type == "DefaultSource") {
        NES_DEBUG("TypeInferencePhase: create default source for one buffer");
        return DefaultSourceDescriptor::create(schema, streamName, numBuffers, frequency);
    } else if (type == "CSVSource") {
        NES_DEBUG("TypeInferencePhase: create CSV source for " << conf << " buffers");
        return CsvSourceDescriptor::create(schema,
                                           streamName,
                                           conf, /**delimiter*/
                                           ",",
                                           numBuffers,
                                           frequency);
    } else if (type == "SenseSource") {
        NES_DEBUG("TypeInferencePhase: create Sense source for udfs " << conf);
        return SenseSourceDescriptor::create(schema, streamName, /**udfs*/ conf);
    } else {
        NES_ERROR("TypeInferencePhase:: source type " << type << " not supported");
        NES_FATAL_ERROR("type not supported");
    }
}

}// namespace NES