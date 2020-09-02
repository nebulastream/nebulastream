#ifndef NES_INCLUDE_MONITORING_METRICS_METRICCATALOG_HPP_
#define NES_INCLUDE_MONITORING_METRICS_METRICCATALOG_HPP_

#include <Monitoring/MetricValues/MetricValueType.hpp>
#include <memory>
#include <string>
#include <map>

namespace NES {
class Metric;
class MetricCatalog;
typedef std::shared_ptr<MetricCatalog> MetricCatalogPtr;

class MetricCatalog {
  public:
    static MetricCatalogPtr create(std::map<MetricValueType, Metric> metrics);

    static MetricCatalogPtr NesMetrics();

    /**
     * @brief Registers a metric.
     * @param name of the metric
     * @param metric metric to register
     * @return true if successful, else false
     */
    bool add(MetricValueType type, const Metric& metric);

  private:
    MetricCatalog(std::map<MetricValueType, Metric> metrics);

  private:
    std::map<MetricValueType, Metric> metrics;
};

}

#endif//NES_INCLUDE_MONITORING_METRICS_METRICCATALOG_HPP_
