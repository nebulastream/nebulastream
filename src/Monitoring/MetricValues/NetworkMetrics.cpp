#include <API/Schema.hpp>
#include <Monitoring/MetricValues/NetworkMetrics.hpp>
#include <Monitoring/Metrics/MetricDefinition.hpp>
#include <NodeEngine/MemoryLayout/RowLayout.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {

NetworkValues& NetworkMetrics::operator[](unsigned int index) {
    if (index >= getInterfaceNum()) {
        NES_THROW_RUNTIME_ERROR("CPU: Array index out of bound " + std::to_string(index) + ">=" + std::to_string(getInterfaceNum()));
    }
    return networkValues[index];
}

void NetworkMetrics::addNetworkValues(NetworkValues&& nwValue) {
    networkValues.emplace_back(nwValue);
}

unsigned int NetworkMetrics::getInterfaceNum() const {
    return networkValues.size();
}

std::vector<std::string> NetworkMetrics::getInterfaceNames() {
    std::vector<std::string> keys;
    keys.reserve(networkValues.size());

    for (auto kv : networkValues) {
        keys.push_back(kv.interfaceName);
    }

    return keys;
}
NetworkMetrics NetworkMetrics::fromBuffer(std::shared_ptr<Schema> schema, TupleBuffer& buf, const std::string& prefix) {
    auto output = NetworkMetrics();
    auto i = schema->getIndex(prefix + "INTERFACE_NO");

    if (i < schema->getSize() && buf.getNumberOfTuples() == 1 && UtilityFunctions::endsWith(schema->fields[i]->name, prefix + "INTERFACE_NO")) {
        auto layout = createRowLayout(schema);
        auto numInt = layout->getValueField<uint16_t>(0, i)->read(buf);

        for (int n=0; n<numInt; n++) {
            auto networkValue = NetworkValues::fromBuffer(schema, buf, prefix + "Intfs[" + std::to_string(i+1) + "]_");
            output.addNetworkValues(std::move(networkValue));
        }
    }
    else {
        NES_THROW_RUNTIME_ERROR("CpuMetrics: Metrics could not be parsed from schema " + schema->toString());
    }

    return output;
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

        for (int i=0; i<metrics.getInterfaceNum(); i++) {
            serialize(metrics[i], schema, buf, def, prefix + "Intfs[" + std::to_string(i+1) + "]_");
        }
    }

}

}// namespace NES