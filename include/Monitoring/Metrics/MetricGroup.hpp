#ifndef NES_INCLUDE_MONITORING_METRICS_METRICGROUP_HPP_
#define NES_INCLUDE_MONITORING_METRICS_METRICGROUP_HPP_

#include <Monitoring/Metrics/Metric.hpp>

#include <memory>
#include <unordered_map>

namespace NES {
class Schema;
class TupleBuffer;

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
    bool add(const std::string& desc, const Metric& metric);

    /**
     * @brief returns a map of the registered metrics
     * @return the metrics map
     */
    template<typename T>
    T& getAs(const std::string& name) const {
        return metricMap.at(name).getValue<T>();
    }

    /**
     * @brief This metric removes a registered metric.
     * @param name of metric
     * @return true if successful, else false
     */
    bool remove(const std::string& name);

    /**
     * @brief Reads the values of all metrics from the MetricGroup and stores them to the passed Schema and TupleBuffer.
     * @param schema
     * @param buf
     */
    void getSample(std::shared_ptr<Schema> schema, TupleBuffer& buf);
  private:
    MetricGroup();
    std::unordered_map<std::string, Metric> metricMap;
};

typedef std::shared_ptr<MetricGroup> MetricGroupPtr;
}// namespace NES

#endif//NES_INCLUDE_MONITORING_METRICS_METRICGROUP_HPP_
