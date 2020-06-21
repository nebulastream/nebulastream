#ifndef NES_INCLUDE_MONITORING_METRICS_METRIC_HPP_
#define NES_INCLUDE_MONITORING_METRICS_METRIC_HPP_

namespace NES {

enum MetricType {
    CounterType,
    GaugeType,
    HistogramType,
    MeterType
};

class Metric {
  public:
    explicit Metric(MetricType metricType);

    MetricType getMetricType() const;

  private:
    MetricType metricType;
};

}

#endif //NES_INCLUDE_MONITORING_METRICS_METRIC_HPP_
