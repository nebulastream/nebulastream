#ifndef NES_INCLUDE_MONITORING_METRICS_METRIC_HPP_
#define NES_INCLUDE_MONITORING_METRICS_METRIC_HPP_

namespace NES {

enum MetricType {
    CounterType,
    GaugeType,
    HistogramType,
    MeterType
};

/**
 * @brief The metric class is a conceptual superclass that represents all metrics in NES.
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

}

#endif //NES_INCLUDE_MONITORING_METRICS_METRIC_HPP_
