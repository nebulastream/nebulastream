#include <Monitoring/MetricValues/NetworkMetrics.hpp>

namespace NES {

NetworkValues& NetworkMetrics::operator[](std::basic_string<char> interfaceName) {
    return interfaceMap[interfaceName];
}

unsigned int NetworkMetrics::size() const {
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

}