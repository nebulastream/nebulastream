#ifndef NES_INCLUDE_MONITORING_METRICVALUES_NETWORKMETRICS_HPP_
#define NES_INCLUDE_MONITORING_METRICVALUES_NETWORKMETRICS_HPP_

#include <Monitoring/MetricValues/NetworkValues.hpp>
#include <cstdint>
#include <unordered_map>
#include <vector>
#include <memory>

namespace NES {

class NetworkMetrics {

  public:
    NetworkMetrics() = default;

    // Overloading [] operator to access elements in array style
    NetworkValues& operator[](std::basic_string<char> interfaceName);

    std::vector<std::string> getInterfaceNames();

    unsigned int size() const;

  private:
    std::unordered_map<std::string, NetworkValues> interfaceMap;
};

typedef std::shared_ptr<NetworkMetrics> NetworkMetricsPtr;

}

#endif //NES_INCLUDE_MONITORING_METRICVALUES_NETWORKMETRICS_HPP_
