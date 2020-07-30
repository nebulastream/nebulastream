#ifndef NES_INCLUDE_MONITORING_METRICS_METRICGROUP_HPP_
#define NES_INCLUDE_MONITORING_METRICS_METRICGROUP_HPP_

#include <unordered_map>
#include <Monitoring/Metrics/Gauge.hpp>
#include <Monitoring/Metrics/Counter.hpp>

namespace NES {

/**
 * A MetricGroup is a named container for Metrics and further metric subgroups.
 *
 * <p>Instances of this class can be used to register new metrics with NES and to create a nested
 * hierarchy based on the group names.
 *
 * <p>A MetricGroup is uniquely identified by its place in the hierarchy and name.
 */
class MetricGroup {
  public:
    static std::shared_ptr<MetricGroup> create();
    ~MetricGroup() = default;

    /**
     * @brief Registers a metric.
     * @param name of the metric
     * @param metric metric to register
     * @return true if successful, else false
     */
    bool addMetric(const std::string& name, Metric* metric);

    /**
     * @brief returns a map of the registered metrics
     * @return the metrics map
     */
    MetricPtr getRegisteredMetric(const std::string& name) const;

  private:
    MetricGroup() = default;

    std::unordered_map<std::string, MetricPtr> metricMap;
};

typedef std::shared_ptr<MetricGroup> MetricGroupPtr;
}

#endif //NES_INCLUDE_MONITORING_METRICS_METRICGROUP_HPP_
