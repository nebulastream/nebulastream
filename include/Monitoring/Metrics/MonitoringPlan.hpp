#ifndef NES_INCLUDE_MONITORING_METRICS_MONITORINGPLAN_HPP_
#define NES_INCLUDE_MONITORING_METRICS_MONITORINGPLAN_HPP_

#include <Monitoring/MetricValues/MetricValueType.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

namespace NES {

class MetricCatalog;
class MetricGroup;
class GroupedValues;

class MonitoringPlan {
  public:
    MonitoringPlan(std::shared_ptr<MetricCatalog> catalog, const std::vector<MetricValueType>& metrics);

    /**
     * @brief Add a specific metric to the plan
     * @param metric
     */
    void addMetric(MetricValueType metric);

    /**
     * @brief Add an array of metrics to the plan
     * @param metrics
     */
    void addMetrics(const std::vector<MetricValueType>& metrics);

    /**
     * @brief Checks if a metric is part of the MonitoringPlan
     * @param metric
     * @return true if contained, else false
     */
    bool hasMetric(MetricValueType metric) const;

    /**
     * @brief creates a MetricGroup out of the MonitoringPlan;
     * @return
     */
    std::shared_ptr<MetricGroup> createMetricGroup() const;

    /**
     * @brief
     * @param schema
     * @param buf
     * @return
     */
    GroupedValues fromBuffer(std::shared_ptr<Schema> schema, TupleBuffer& buf);

  public:
    static const std::string CPU_METRICS_DESC;
    static const std::string CPU_VALUES_DESC;
    static const std::string NETWORK_METRICS_DESC;
    static const std::string NETWORK_VALUES_DESC;
    static const std::string MEMORY_METRICS_DESC;
    static const std::string DISK_METRICS_DESC;

  private:
    std::shared_ptr<MetricCatalog> catalog;

  private:
    //the metrics for monitoring
    bool cpuMetrics;
    bool networkMetrics;
    bool memoryMetrics;
    bool diskMetrics;
};

}// namespace NES

#endif//NES_INCLUDE_MONITORING_METRICS_MONITORINGPLAN_HPP_
