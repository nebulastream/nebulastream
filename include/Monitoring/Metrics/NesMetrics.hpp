#ifndef NES_INCLUDE_MONITORING_METRICS_NESMETRICS_HPP_
#define NES_INCLUDE_MONITORING_METRICS_NESMETRICS_HPP_

#include <unordered_map>
#include <Monitoring/Metrics/Gauge.hpp>

namespace NES {

class NesMetrics {
  public:
    static Gauge<std::unordered_map<std::string, uint64_t>> CPUStats();

  private:
    NesMetrics() = default;

};

}

#endif //NES_INCLUDE_MONITORING_METRICS_NESMETRICS_HPP_
