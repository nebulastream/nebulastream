#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/Metrics/MetricDefinition.hpp>
#include <NodeEngine/MemoryLayout/RowLayout.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>

namespace NES {

void serialize(uint64_t metric, std::shared_ptr<Schema> schema, TupleBuffer& buf, MetricDefinition& def,
               const std::string& prefix) {
    if (def.simpleTypedMetrics.find(prefix) != def.simpleTypedMetrics.end()) {
        //element is already in MetricDefinition
        NES_THROW_RUNTIME_ERROR("MemoryMetrics: Error during serialize(..): Metric with " +
            prefix + " is already in MetricDefinition.");
    }
    else {
        def.simpleTypedMetrics.insert(prefix);

        auto noFields = schema->getSize();
        schema->addField(prefix, BasicType::UINT64);
        buf.setNumberOfTuples(1);
        auto layout = createRowLayout(schema);
        layout->getValueField<uint64_t>(0, noFields)->write(buf, metric);
    }
}

void serialize(std::string metric, std::shared_ptr<Schema> schema, TupleBuffer&, MetricDefinition&,
               const std::string& prefix) {
    NES_THROW_RUNTIME_ERROR("Metric: Serialization for std::string not possible for metric " + metric + "and schema " + prefix + schema->toString());
}

}// namespace NES