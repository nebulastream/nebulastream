#ifndef NES_INCLUDE_MONITORING_METRICVALUES_NETWORKMETRICS_HPP_
#define NES_INCLUDE_MONITORING_METRICVALUES_NETWORKMETRICS_HPP_

#include <Monitoring/MetricValues/NetworkValues.hpp>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

namespace NES {
class Schema;
class TupleBuffer;

class NetworkMetrics {

  public:
    NetworkMetrics() = default;

    // Overloading [] operator to access elements in array style
    NetworkValues& operator[](std::basic_string<char> interfaceName);

    std::vector<std::string> getInterfaceNames();

    unsigned int getIntfsNo() const;

  private:
    std::unordered_map<std::string, NetworkValues> interfaceMap;
};

void serialize(NetworkMetrics, std::shared_ptr<Schema>, TupleBuffer&, const std::string& prefix="");


}// namespace NES

#endif//NES_INCLUDE_MONITORING_METRICVALUES_NETWORKMETRICS_HPP_
