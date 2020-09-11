#include <API/Schema.hpp>
#include <Monitoring/MetricValues/NetworkMetrics.hpp>
#include <NodeEngine/MemoryLayout/RowLayout.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {

NetworkValues NetworkMetrics::getNetworkValue(uint64_t interfaceNo) const {
    if (interfaceNo >= getInterfaceNum()) {
        NES_THROW_RUNTIME_ERROR("CPU: Array index out of bound " + std::to_string(interfaceNo) + ">=" + std::to_string(getInterfaceNum()));
    }
    return networkValues.at(interfaceNo);
}

void NetworkMetrics::addNetworkValues(NetworkValues&& nwValue) {
    networkValues.emplace_back(nwValue);
}

uint64_t NetworkMetrics::getInterfaceNum() const {
    return networkValues.size();
}

std::vector<std::string> NetworkMetrics::getInterfaceNames() {
    std::vector<std::string> keys;
    keys.reserve(networkValues.size());

    for (const auto& netVal : networkValues) {
        keys.push_back(netVal.interfaceName);
    }

    return keys;
}

NetworkMetrics NetworkMetrics::fromBuffer(SchemaPtr schema, TupleBuffer& buf, const std::string& prefix) {
    auto output = NetworkMetrics();
    auto i = schema->getIndex(prefix + "INTERFACE_NO");

    if (i < schema->getSize() && buf.getNumberOfTuples() == 1 && UtilityFunctions::endsWith(schema->fields[i]->name, prefix + "INTERFACE_NO")) {
        NES_DEBUG("NetworkMetrics: Prefix found in schema " + prefix + "INTERFACE_NO with index " + std::to_string(i));
        auto layout = createRowLayout(schema);
        auto numInt = layout->getValueField<uint16_t>(0, i)->read(buf);

        for (int n = 0; n < numInt; n++) {
            NES_DEBUG("NetworkMetrics: Parsing buffer for interface " + prefix + "Intfs[" + std::to_string(n + 1) + "]_");

            auto networkValue = NetworkValues::fromBuffer(schema, buf, prefix + "Intfs[" + std::to_string(n + 1) + "]_");
            output.addNetworkValues(std::move(networkValue));
        }
    } else {
        NES_THROW_RUNTIME_ERROR("NetworkMetrics: Metrics could not be parsed from schema " + schema->toString());
    }

    return output;
}

void serialize(const NetworkMetrics& metrics, SchemaPtr schema, TupleBuffer& buf, const std::string& prefix) {
    auto noFields = schema->getSize();
    schema->addField(prefix + "INTERFACE_NO", BasicType::UINT16);
    buf.setNumberOfTuples(1);

    auto layout = createRowLayout(schema);
    layout->getValueField<uint16_t>(0, noFields)->write(buf, metrics.getInterfaceNum());

    for (int i = 0; i < metrics.getInterfaceNum(); i++) {
        serialize(metrics.getNetworkValue(i), schema, buf, prefix + "Intfs[" + std::to_string(i + 1) + "]_" + UtilityFunctions::trim(metrics.getNetworkValue(i).interfaceName) + "_");
    }
}

}// namespace NES