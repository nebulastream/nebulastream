#ifndef NES_INCLUDE_MONITORING_METRICS_METRICGROUP_HPP_
#define NES_INCLUDE_MONITORING_METRICS_METRICGROUP_HPP_

#include <unordered_map>
#include <Monitoring/Metrics/Metric.hpp>

namespace NES {

/**
 * A MetricGroup is a named container for Metrics and further metric subgroups.
 *
 * <p>Instances of this class can be used to register new metrics with NES and to create a nested
 * hierarchy based on the group names.
 *
 * <p>A MetricGroup is uniquely identified by it's place in the hierarchy and name.
 */
class MetricGroup {
  public:
    MetricGroup() = default;
    ~MetricGroup() = default;

    /**
     * @brief Registers a metric.
     * @param id of the metric
     * @param metric metric to register
     * @return true if successful, else false
     */
    bool add(const std::string& id, Metric* metric);

    /**
     * @brief returns a map of the registered metrics
     * @return the metrics map
     */
    std::unordered_map<std::string, Metric*> getRegisteredMetrics() const;

  private:
    std::unordered_map<std::string, Metric*> metricMap;

};

}

#endif //NES_INCLUDE_MONITORING_METRICS_METRICGROUP_HPP_
