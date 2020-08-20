#ifndef NES_INCLUDE_MONITORING_METRICS_METRICDEFINITION_HPP_
#define NES_INCLUDE_MONITORING_METRICS_METRICDEFINITION_HPP_

#include <string>
#include <set>

namespace NES {

class MetricDefinition {
  public:
    MetricDefinition() = default;
    std::set<std::string> simpleTypedMetrics;
    std::set<std::string> diskMetrics;
    std::set<std::string> cpuMetrics;
    std::set<std::string> cpuValues;

    std::set<std::string> networkMetrics;
    std::set<std::string> networkValues;

    std::set<std::string> memoryMetrics;
};

}

#endif//NES_INCLUDE_MONITORING_METRICS_METRICDEFINITION_HPP_
