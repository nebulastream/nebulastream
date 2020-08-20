#include <Monitoring/MetricValues/NetworkMetrics.hpp>
#include <Util/Logger.hpp>
#include <API/Schema.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <NodeEngine/MemoryLayout/RowLayout.hpp>
#include <Monitoring/Metrics/MetricDefinition.hpp>

namespace NES {

NetworkValues& NetworkMetrics::operator[](std::basic_string<char> interfaceName) {
    return interfaceMap[interfaceName];
}

unsigned int NetworkMetrics::getInterfaceNum() const {
    return interfaceMap.size();
}

std::vector<std::string> NetworkMetrics::getInterfaceNames() {
    std::vector<std::string> keys;
    keys.reserve(interfaceMap.size());

    for (auto kv : interfaceMap) {
        keys.push_back(kv.first);
    }

    return keys;
}

void serialize(NetworkMetrics metrics, std::shared_ptr<Schema> schema, TupleBuffer& buf, MetricDefinition& def,
               const std::string& prefix) {
    if (def.networkMetrics.find(prefix) != def.networkMetrics.end()) {
        //element is already in MetricDefinition
        NES_THROW_RUNTIME_ERROR("MemoryMetrics: Error during serialize(..): Metric with " +
            prefix + " is already in MetricDefinition.");
    }
    else {
        def.networkMetrics.insert(prefix);

        auto noFields = schema->getSize();
        schema->addField(prefix + "INTERFACE_NO", BasicType::UINT16);
        buf.setNumberOfTuples(1);

        auto layout = createRowLayout(schema);
        layout->getValueField<uint16_t>(0, noFields)->write(buf, metrics.getInterfaceNum());

        for (auto intfsName: metrics.getInterfaceNames()) {
            serialize(metrics[intfsName], schema, buf, def, prefix + "Intfs[" + intfsName + "]_");
        }
    }

}

}// namespace NES