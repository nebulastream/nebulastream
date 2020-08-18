#include <Monitoring/MetricValues/NetworkMetrics.hpp>
#include <Util/Logger.hpp>
#include <API/Schema.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <NodeEngine/MemoryLayout/RowLayout.hpp>

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

void serialize(NetworkMetrics metrics, std::shared_ptr<Schema> schema, TupleBuffer& buf, const std::string& prefix) {
    auto noFields = schema->getSize();
    schema->addField(prefix + "INTERFACE_NO", BasicType::UINT16);
    buf.setNumberOfTuples(1);

    auto layout = createRowLayout(schema);
    layout->getValueField<uint16_t>(0, noFields)->write(buf, metrics.getInterfaceNum());

    for (auto intfsName: metrics.getInterfaceNames()) {
        serialize(metrics[intfsName], schema, buf, prefix + "Intfs[" + intfsName + "]_");
    }
}

}// namespace NES