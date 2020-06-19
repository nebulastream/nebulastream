#ifndef NES_INCLUDE_MONITORING_METRICS_METRICGROUP_HPP_
#define NES_INCLUDE_MONITORING_METRICS_METRICGROUP_HPP_

#include <Monitoring/Metrics/Counter.hpp>
#include <Monitoring/Metrics/Gauge.hpp>
#include <unordered_map>

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
    MetricGroup();

    ~MetricGroup();

    /**
     * @brief Registers a Counter.
     * @param id of the counter
     * @param counter counter to register
     * @return the given counter
     */
    Counter& counter(uint64_t id, Counter& counter);

    /**
      * @brief Registers a Gauge.
      * @param id of the Gauge
      * @param gauge Gauge to register
      * @return the given Gauge
      */
    template<typename T>
    Gauge<T>& gauge(uint64_t id, Gauge<T>& gauge);

    /**
     * @brief returns a map of the registered metrics
     * @return the metrics map
     */
    std::unordered_map<uint64_t, Metric&>& getRegisteredMetrics();

  private:
    std::unordered_map<uint64_t, Metric&> metricMap;

};

}

#endif //NES_INCLUDE_MONITORING_METRICS_METRICGROUP_HPP_
