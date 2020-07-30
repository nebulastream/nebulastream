#ifndef NES_INCLUDE_MONITORING_METRICS_METRIC_HPP_
#define NES_INCLUDE_MONITORING_METRICS_METRIC_HPP_

#include <memory>
namespace NES {

/**
 * @brief The metric types of NES
 * Counter for incrementing and decrementing values
 * Gauge for reading and returning a specific value
 * Histogram that creates a histogram over time
 * Meter that measures an interval between two points in time
 */
enum MetricType {
    CounterType,
    GaugeType,
    HistogramType,
    MeterType
};

/**
 * @brief The metric class is a conceptual superclass that represents all metrics in NES.
 * Currently existing metrics are Counter, Gauge, Histogram and Meter.
 */
class Metric {
  public:
    explicit Metric(MetricType metricType);

    /**
     * @brief Returns the type a metric is representing, e.g., CounterType, GaugeType..
     * @return the metric type
     */
    MetricType getMetricType() const;

  private:
    MetricType metricType;
};

typedef std::shared_ptr<Metric> MetricPtr;

}

#endif //NES_INCLUDE_MONITORING_METRICS_METRIC_HPP_
